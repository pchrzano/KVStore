#include "log.hh"
#include "recovery.hh"
#include "webapi.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/defer.hh>

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
        // Handlers can't be unregistered; replace them with no-ops.
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
            stop_signal stop;
            const auto& cfg = app.configuration();
            const auto address = cfg["address"].as<ss::sstring>();
            const auto port = cfg["port"].as<uint16_t>();
            const auto cache_size = cfg["cache-size"].as<size_t>();

            // Every shard's WebApi is constructed with cache_size.
            ss::sharded<WebApi> api;
            api.start(cache_size).get();
            kvlog.info("cache: {} key-value pairs per shard", cache_size);
            auto stop_api = ss::defer([&api]() noexcept { api.stop().get(); });

            // Load (or reshard) the data files before serving anything.
            recover(api).get();

            // http_server_control runs one HTTP server instance per shard.
            ss::httpd::http_server_control server;
            server.start("kv").get();
            auto stop_server = ss::defer([&server]() noexcept { server.stop().get(); });

            // Hand request bodies to handlers as req->content_stream instead of
            // pre-reading them into the deprecated req->content.
            server.server().invoke_on_all([](ss::httpd::http_server& s) {
                s.set_content_streaming(true);
            }).get();

            // set_routes runs this on every shard; local() is that shard's WebApi.
            server.set_routes([&api](ss::httpd::routes& r) { api.local().register_routes(r); }).get();

            server.listen(ss::socket_address{ss::net::inet_address{address}, port}).get();
            kvlog.info("KVApp started");

            stop.wait().get();
            return 0;
        });
    });
}
