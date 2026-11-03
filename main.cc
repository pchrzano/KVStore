#include "log.hh"
#include "store_loader.hh"
#include "webapi.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <csignal>

namespace ss = seastar;
namespace bpo = boost::program_options;

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
            "key-value pairs cached per shard (0 disables the cache)");

    return app.run(argc, argv, [&app] {
        return ss::async([&app] {
            const auto started = std::chrono::steady_clock::now();
            stop_signal stop;
            const auto& cfg = app.configuration();
            const auto address = cfg["address"].as<ss::sstring>();
            const auto port = cfg["port"].as<uint16_t>();
            const auto cache_size = cfg["cache-size"].as<size_t>();

            kvlog.info("starting: smp={} address={} port={} cache_size={}",
                    ss::this_smp_shard_count(), address, port, cache_size);
            if (cfg.count("unsafe-bypass-fsync") && cfg["unsafe-bypass-fsync"].as<bool>()) {
                kvlog.warn("fsync is bypassed (--unsafe-bypass-fsync), data may be lost on crash");
            }

            ss::sharded<WebApi> api;
            api.start(cache_size).get();
            auto stop_api = ss::defer([&api]() noexcept { api.stop().get(); });

            StoreLoader(api).load_or_reshard().get();

            ss::httpd::http_server_control server;
            server.start("kv").get();
            auto stop_server = ss::defer([&server]() noexcept { server.stop().get(); });

            server.server().invoke_on_all([](ss::httpd::http_server& s) {
                s.set_content_streaming(true);
            }).get();

            server.set_routes([&api](ss::httpd::routes& r) { api.local().register_routes(r); }).get();

            server.listen(ss::socket_address{ss::net::inet_address{address}, port}).get();
            kvlog.info("listening on {}:{} (startup took {:.0f} ms)", address, port, elapsed_ms(started));

            stop.wait().get();
            kvlog.info("stopping");
            return 0;
        }).then([](int code) {
            kvlog.info("stopped");
            return code;
        });
    });
}
