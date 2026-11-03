#pragma once

#include "store.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/http/routes.hh>

#include <memory>
#include <string>
#include <string_view>

// One instance per shard (seastar::sharded<WebApi>). peering_sharded_service
// provides container(), so a handler can call the instance on another shard.
class WebApi : public seastar::peering_sharded_service<WebApi> {
public:
    // `cache_capacity`: key-value pairs cached by this shard's Store.
    explicit WebApi(std::size_t cache_capacity) : _store(cache_capacity) {}

    void register_routes(seastar::httpd::routes& r);

    // This shard's Store (used at startup by recover()).
    Store& store() noexcept { return _store; }

    // Required by seastar::sharded<>.
    seastar::future<> stop() { return _store.stop(); }

private:
    using request_ptr = std::unique_ptr<seastar::http::request>;
    using reply_ptr = std::unique_ptr<seastar::http::reply>;
    using endpoint = seastar::future<reply_ptr> (WebApi::*)(request_ptr, reply_ptr);

    // PUT /api/v1/kv/{key}   body = value; creates the key or updates it
    seastar::future<reply_ptr> put(request_ptr req, reply_ptr rep);

    // GET /api/v1/kv/{key}
    seastar::future<reply_ptr> get(request_ptr req, reply_ptr rep);

    // GET /api/v1/kv         all keys, sorted
    seastar::future<reply_ptr> list_keys(request_ptr req, reply_ptr rep);

    // DELETE /api/v1/kv/{key}
    seastar::future<reply_ptr> delete_key(request_ptr req, reply_ptr rep);

    // Wraps a member endpoint in a Seastar handler that replies with JSON.
    seastar::httpd::handler_base* make_handler(endpoint fn);

    // The shard whose Store holds this key.
    static unsigned owner(const Key& key);

    static std::string json_escape(std::string_view in);

    Store _store;
};
