#include "recovery.hh"

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
#include <charconv>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace ss = seastar;

namespace {

constexpr char meta_file[] = "kv.meta";
constexpr char meta_tmp_file[] = "kv.meta.tmp";
constexpr unsigned format_version = 1;

// Resharding sends records to their new owner in batches of about this size.
constexpr size_t batch_bytes = 1 << 20;
constexpr size_t batch_records = 4096;

struct Meta {
    unsigned shard_count;
    uint64_t generation;
};

std::optional<uint64_t> parse_uint(std::string_view s) {
    uint64_t v;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || ptr != s.data() + s.size()) {
        return std::nullopt;
    }
    return v;
}

// kv.meta is text, one `name=value` per line.
ss::future<std::optional<Meta>> read_meta() {
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

// Write to a temporary file, fsync, rename over kv.meta, fsync the directory:
// a crash leaves either the old or the new kv.meta, never a partial one.
ss::future<> write_meta(Meta m) {
    const ss::sstring text = ss::format("format={}\nshard_count={}\ngeneration={}\n",
            format_version, m.shard_count, m.generation);
    ss::file f = co_await ss::open_file_dma(meta_tmp_file,
            ss::open_flags::wo | ss::open_flags::create | ss::open_flags::truncate);
    auto out = co_await ss::make_file_output_stream(std::move(f));
    std::exception_ptr ex;
    try {
        co_await out.write(text);
        co_await out.flush(); // includes fdatasync
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close(); // closes the file
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_await ss::rename_file(meta_tmp_file, meta_file);
    co_await ss::sync_directory(".");
}

// Generation of a data file name "store-g<generation>-<shard>.dat".
std::optional<uint64_t> data_file_generation(std::string_view name) {
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

ss::future<> remove_other_generations(uint64_t keep) {
    std::vector<ss::sstring> stale;
    ss::file dir = co_await ss::open_directory(".");
    std::exception_ptr ex;
    try {
        auto lister = dir.list_directory([&stale, keep](ss::directory_entry de) {
            auto gen = data_file_generation(de.name);
            if (gen && *gen != keep) {
                stale.push_back(de.name);
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
    for (const auto& name : stale) {
        kvlog.info("removing {} (not generation {})", name, keep);
        co_await ss::remove_file(name);
    }
    if (!stale.empty()) {
        co_await ss::sync_directory(".");
    }
}

// Runs on the shard that was assigned the old file: replays it and sends
// every live record to its owner under the new shard count.
ss::future<> migrate_file(ss::sharded<WebApi>& api, ss::sstring path, unsigned new_count) {
    if (!co_await ss::file_exists(path)) {
        co_return; // that old shard never wrote anything
    }
    Store::ScanResult scan = co_await Store::scan(path);
    if (scan.valid_end < scan.file_size) {
        kvlog.warn("{}: ignoring {} bytes of incomplete or invalid data at offset {}",
                path, scan.file_size - scan.valid_end, scan.valid_end);
    }

    // Read the values in file order, so the reads go forward through the file.
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
    kvlog.info("{}: moved {} keys", path, by_offset.size());
}

ss::future<> reshard(ss::sharded<WebApi>& api, Meta old, unsigned new_count) {
    const uint64_t new_gen = old.generation + 1;
    kvlog.info("resharding from {} to {} shards (generation {} -> {})",
            old.shard_count, new_count, old.generation, new_gen);

    co_await api.invoke_on_all([new_gen](WebApi& a) {
        a.store().start_generation(new_gen);
    });
    // Old file i is migrated by new shard i % new_count, all shards in parallel.
    co_await api.invoke_on_all([&api, old, new_count](WebApi&) -> ss::future<> {
        for (unsigned i = ss::this_shard_id(); i < old.shard_count; i += new_count) {
            co_await migrate_file(api, Store::file_name(old.generation, i), new_count);
        }
    });

    // Every record is now fsynced in the new generation: switch over.
    co_await write_meta({new_count, new_gen});
    co_await remove_other_generations(new_gen);
}

} // namespace

ss::future<> recover(ss::sharded<WebApi>& api) {
    const unsigned count = ss::this_smp_shard_count();
    std::optional<Meta> meta = co_await read_meta();
    if (!meta) {
        kvlog.info("new data directory: {} shards, generation 1", count);
        co_await write_meta({count, 1});
        co_await remove_other_generations(1);
        co_await api.invoke_on_all([](WebApi& a) { a.store().start_generation(1); });
        co_return;
    }

    co_await remove_other_generations(meta->generation);
    if (meta->shard_count == count) {
        co_await api.invoke_on_all([gen = meta->generation](WebApi& a) {
            return a.store().load(gen);
        });
    } else {
        co_await reshard(api, *meta, count);
    }
}
