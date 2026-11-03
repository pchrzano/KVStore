#include "log.hh"
#include "store_loader.hh"
#include "webapi.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <csignal>

namespace ss = seastar;
namespace bpo = boost::program_options;

constexpr uint64_t values_per_get = 8;

bool enough_memory(uint64_t cache_size, uint64_t max_value_size, uint64_t memory_per_shard) {
    using u128 = unsigned __int128;
    const u128 budget = memory_per_shard / 2;
    const u128 worst_case = (u128(cache_size) + values_per_get) * max_value_size;
    if (worst_case <= budget) {
        return true;
    }
    auto clamp = [](u128 v) { return static_cast<uint64_t>(std::min<u128>(v, UINT64_MAX)); };
    std::string fixes = fmt::format("--max-value-size {} or less", clamp(budget / (u128(cache_size) + values_per_get)));
    if (max_value_size > 0 && budget / max_value_size >= values_per_get) {
        fixes += fmt::format(", --cache-size {} or less", clamp(budget / max_value_size - values_per_get));
    }
    kvlog.error("not enough memory per shard: a full cache ({} values) plus one GET ({} copies) of {}-byte values "
            "needs {} bytes, but only half of the {} bytes per shard is available for values. "
            "Use {}, or at least {} bytes of memory per shard (--memory / --smp)",
            cache_size, values_per_get, max_value_size, clamp(worst_case), memory_per_shard,
            fixes, clamp(2 * worst_case));
    return false;
}

class stop_signal {
    bool _caught = false;
    ss::condition_variable _cond;

    void signaled() {
        if (!_caught) {
            _caught = true;
            _cond.broadcast();
        }
    }

public:
    stop_signal() {
        ss::handle_signal(SIGINT, [this] { signaled(); });
        ss::handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        ss::handle_signal(SIGINT, [] {});
        ss::handle_signal(SIGTERM, [] {});
    }
    ss::future<> wait() { return _cond.wait([this] { return _caught; }); }
};

int main(int argc, char** argv) {
    ss::app_template app;
    app.add_options()
        ("address", bpo::value<ss::sstring>()->default_value("0.0.0.0"), "HTTP listen address")
        ("port", bpo::value<uint16_t>()->default_value(8080), "HTTP listen port")
        ("cache-size", bpo::value<size_t>()->default_value(LRUCache::default_capacity),
            "key-value pairs cached per shard (0 disables the cache)")
        ("max-value-size", bpo::value<size_t>()->default_value(1 << 20),
            "maximum value size in bytes; larger PUTs get 413");

    return app.run(argc, argv, [&app] {
        return ss::async([&app] {
            const auto started = std::chrono::steady_clock::now();
            stop_signal stop;
            const auto& cfg = app.configuration();
            const auto address = cfg["address"].as<ss::sstring>();
            const auto port = cfg["port"].as<uint16_t>();
            const auto cache_size = cfg["cache-size"].as<size_t>();
            const auto max_value_size = cfg["max-value-size"].as<size_t>();
            const size_t memory_per_shard = ss::memory::stats().total_memory();

            kvlog.info("starting: smp={} address={} port={} cache_size={} max_value_size={} memory_per_shard={}",
                    ss::this_smp_shard_count(), address, port, cache_size, max_value_size, memory_per_shard);
            if (!enough_memory(cache_size, max_value_size, memory_per_shard)) {
                return 1;
            }
            if (cfg.count("unsafe-bypass-fsync") && cfg["unsafe-bypass-fsync"].as<bool>()) {
                kvlog.warn("fsync is bypassed (--unsafe-bypass-fsync), data may be lost on crash");
            }

            ss::sharded<WebApi> api;
            api.start(cache_size, max_value_size).get();
            auto stop_api = ss::defer([&api]() noexcept { api.stop().get(); });

            StoreLoader(api).load_or_reshard().get();

            ss::httpd::http_server_control server;
            server.start("kv").get();
            auto stop_server = ss::defer([&server]() noexcept { server.stop().get(); });

            server.server().invoke_on_all([max_value_size](ss::httpd::http_server& s) {
                s.set_content_streaming(true);
                s.set_content_length_limit(max_value_size);
            }).get();

            server.set_routes([&api](ss::httpd::routes& r) { api.local().register_routes(r); }).get();

            server.listen(ss::socket_address{ss::net::inet_address{address}, port}).get();
            kvlog.info("listening on {}:{} (startup took {:.0f} ms)", address, port, elapsed_ms(started));

            stop.wait().get();
            kvlog.info("stopping");
            return 0;
        }).then([](int code) {
            if (code == 0) {
                kvlog.info("stopped");
            }
            return code;
        });
    });
}
