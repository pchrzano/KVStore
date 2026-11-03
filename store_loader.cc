#include "store_loader.hh"

#include "log.hh"
#include "store.hh"
#include "webapi.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/format.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/file.hh>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iterator>
#include <string>
#include <charconv>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace ss = seastar;

namespace {

std::string format_bytes(uint64_t bytes) {
    constexpr const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    double v = static_cast<double>(bytes);
    size_t u = 0;
    while (v >= 1024 && u + 1 < std::size(units)) {
        v /= 1024;
        ++u;
    }
    return u == 0 ? fmt::format("{} B", bytes) : fmt::format("{:.1f} {}", v, units[u]);
}

}

std::optional<uint64_t> StoreLoader::parse_uint(std::string_view s) {
    uint64_t v;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || ptr != s.data() + s.size()) {
        return std::nullopt;
    }
    return v;
}

ss::future<std::optional<StoreLoader::Meta>> StoreLoader::read_meta() {
    if (!co_await ss::file_exists(meta_file)) {
        co_return std::nullopt;
    }
    ss::sstring text = co_await ss::util::read_entire_file_contiguous(meta_file);
    std::optional<uint64_t> format, shards, generation;
    std::string_view rest(text);
    while (!rest.empty()) {
        auto nl = rest.find('\n');
        std::string_view line = rest.substr(0, nl);
        rest = nl == std::string_view::npos ? std::string_view{} : rest.substr(nl + 1);
        auto eq = line.find('=');
        if (eq == std::string_view::npos) {
            continue;
        }
        auto name = line.substr(0, eq);
        auto value = parse_uint(line.substr(eq + 1));
        if (name == "format") {
            format = value;
        } else if (name == "shard_count") {
            shards = value;
        } else if (name == "generation") {
            generation = value;
        }
    }
    if (format != format_version || !shards || *shards == 0 || !generation || *generation == 0) {
        throw std::runtime_error(ss::format("{}: unsupported or invalid contents", meta_file));
    }
    co_return Meta{static_cast<unsigned>(*shards), *generation};
}

ss::future<> StoreLoader::write_meta(Meta m) {
    const ss::sstring text = ss::format("format={}\nshard_count={}\ngeneration={}\n",
            format_version, m.shard_count, m.generation);
    ss::file f = co_await ss::open_file_dma(meta_tmp_file,
            ss::open_flags::wo | ss::open_flags::create | ss::open_flags::truncate);
    auto out = co_await ss::make_file_output_stream(std::move(f));
    std::exception_ptr ex;
    try {
        co_await out.write(text);
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_await ss::rename_file(meta_tmp_file, meta_file);
    co_await ss::sync_directory(".");
}

std::optional<uint64_t> StoreLoader::data_file_generation(std::string_view name) {
    constexpr std::string_view prefix = "store-g", suffix = ".dat";
    if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
        return std::nullopt;
    }
    name.remove_prefix(prefix.size());
    name.remove_suffix(suffix.size());
    auto dash = name.find('-');
    if (dash == std::string_view::npos || !parse_uint(name.substr(dash + 1))) {
        return std::nullopt;
    }
    return parse_uint(name.substr(0, dash));
}

ss::future<std::vector<ss::sstring>> StoreLoader::list_data_files() {
    std::vector<ss::sstring> files;
    ss::file dir = co_await ss::open_directory(".");
    std::exception_ptr ex;
    try {
        auto lister = dir.list_directory([&files](ss::directory_entry de) {
            if (data_file_generation(de.name)) {
                files.push_back(de.name);
            }
            return ss::make_ready_future<>();
        });
        co_await lister.done();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await dir.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    std::ranges::sort(files);
    co_return files;
}

ss::future<> StoreLoader::remove_other_generations(uint64_t keep, bool replaced) {
    std::vector<ss::sstring> stale = co_await list_data_files();
    std::erase_if(stale, [keep](const ss::sstring& name) { return data_file_generation(name) == keep; });
    for (const auto& name : stale) {
        if (replaced) {
            loaderlog.info("removing {}: replaced by generation {}", name, keep);
        } else {
            loaderlog.warn("removing {}: not part of generation {} (left over from an interrupted resharding)",
                    name, keep);
        }
        co_await ss::remove_file(name);
    }
    if (!stale.empty()) {
        co_await ss::sync_directory(".");
    }
}

ss::future<size_t> StoreLoader::migrate_file(ss::sharded<WebApi>& api, ss::sstring path, unsigned new_count) {
    if (!co_await ss::file_exists(path)) {
        co_return 0;
    }
    Store::ScanResult scan = co_await Store::scan(path);
    if (scan.valid_end < scan.file_size) {
        loaderlog.warn("{}: ignoring {} bytes of incomplete or invalid data at offset {}",
                path, scan.file_size - scan.valid_end, scan.valid_end);
    }

    std::vector<std::pair<uint64_t, Key>> by_offset;
    by_offset.reserve(scan.live.size());
    for (auto& [key, offset] : scan.live) {
        by_offset.emplace_back(offset, key);
    }
    scan.live.clear();
    std::ranges::sort(by_offset, {}, &std::pair<uint64_t, Key>::first);

    std::vector<std::vector<std::pair<Key, Value>>> batches(new_count);
    std::vector<size_t> bytes(new_count, 0);
    auto send = [&](unsigned dest) -> ss::future<> {
        auto batch = std::exchange(batches[dest], {});
        bytes[dest] = 0;
        co_await api.invoke_on(dest, [batch = std::move(batch)](WebApi& a) mutable {
            return a.store().import(std::move(batch));
        });
    };

    ss::file f = co_await ss::open_file_dma(path, ss::open_flags::ro);
    std::exception_ptr ex;
    try {
        for (auto& [offset, key] : by_offset) {
            Value value = co_await Store::read_value(f, path, offset, key);
            const unsigned dest = Store::owner(key, new_count);
            bytes[dest] += key.size() + value.size();
            batches[dest].emplace_back(key, std::move(value));
            if (bytes[dest] >= batch_bytes || batches[dest].size() >= batch_records) {
                co_await send(dest);
            }
        }
        for (unsigned dest = 0; dest < new_count; ++dest) {
            if (!batches[dest].empty()) {
                co_await send(dest);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    loaderlog.debug("{}: moved {} keys", path, by_offset.size());
    co_return by_offset.size();
}

ss::future<> StoreLoader::reshard(Meta old, unsigned new_count) {
    const auto started = std::chrono::steady_clock::now();
    const uint64_t new_gen = old.generation + 1;
    _stage = ss::format("resharding {}→{} (generation {} → {}); kv.meta still at generation {}",
            old.shard_count, new_count, old.generation, new_gen, old.generation);

    co_await _api.invoke_on_all([new_gen](WebApi& a) {
        a.store().start_generation(new_gen);
    });
    const size_t moved = co_await _api.map_reduce0(
            [&api = _api, old, new_count](WebApi&) -> ss::future<size_t> {
                size_t n = 0;
                for (unsigned i = ss::this_shard_id(); i < old.shard_count; i += new_count) {
                    n += co_await migrate_file(api, Store::file_name(old.generation, i), new_count);
                }
                co_return n;
            },
            size_t{0}, std::plus<>{});

    co_await write_meta({new_count, new_gen});
    loaderlog.info("kv.meta switched to generation {}, shard count {}", new_gen, new_count);

    _stage = ss::format("removing generation {} files after resharding", old.generation);
    co_await remove_other_generations(new_gen, true);
    loaderlog.info("resharded {}→{}: {} keys moved in {:.0f} ms",
            old.shard_count, new_count, moved, elapsed_ms(started));
}

ss::future<> StoreLoader::load_or_reshard() {
    const auto started = std::chrono::steady_clock::now();
    const unsigned count = ss::this_smp_shard_count();
    _stage = "reading kv.meta";
    try {
        std::optional<Meta> meta = co_await read_meta();
        if (!meta) {
            _stage = "checking the data directory";
            const std::vector<ss::sstring> files = co_await list_data_files();
            if (!files.empty()) {
                throw std::runtime_error(ss::format(
                        "kv.meta is missing but {} data files exist ({}{}); restore kv.meta, or move the data "
                        "files away to start with an empty store",
                        files.size(), files.front(), files.size() > 1 ? ", ..." : ""));
            }
            loaderlog.info("no kv.meta: new data directory, {} shards, generation 1", count);
            _stage = "creating a new data directory";
            co_await write_meta({count, 1});
            co_await _api.invoke_on_all([](WebApi& a) { a.store().start_generation(1); });
            co_return;
        }

        const bool same = meta->shard_count == count;
        loaderlog.info("kv.meta: generation {}, {} shards; running with {} → {}",
                meta->generation, meta->shard_count, count, same ? "loading" : "resharding");

        _stage = ss::format("removing leftover files (kv.meta at generation {})", meta->generation);
        co_await remove_other_generations(meta->generation, false);

        if (same) {
            _stage = ss::format("loading generation {}", meta->generation);
            const Store::LoadStats stats = co_await _api.map_reduce0(
                    [gen = meta->generation](WebApi& a) { return a.store().load(gen); },
                    Store::LoadStats{}, std::plus<>{});
            loaderlog.info("loaded {} keys ({} records, {}) in {:.0f} ms",
                    stats.keys, stats.records, format_bytes(stats.bytes), elapsed_ms(started));
        } else {
            co_await reshard(*meta, count);
        }
    } catch (...) {
        loaderlog.error("startup failed while {}: {}", _stage, ss::formattable(std::current_exception()));
        throw;
    }
}
