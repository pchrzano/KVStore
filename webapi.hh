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

class WebApi : public seastar::peering_sharded_service<WebApi> {
public:
    explicit WebApi(std::size_t cache_capacity) : _store(cache_capacity) {}

    void register_routes(seastar::httpd::routes& r);

    Store& store() noexcept { return _store; }

    seastar::future<> stop() { return _store.stop(); }

private:
    using request_ptr = std::unique_ptr<seastar::http::request>;
    using reply_ptr = std::unique_ptr<seastar::http::reply>;
    using endpoint = seastar::future<reply_ptr> (WebApi::*)(request_ptr, reply_ptr);

    seastar::future<reply_ptr> put(request_ptr req, reply_ptr rep);

    seastar::future<reply_ptr> get(request_ptr req, reply_ptr rep);

    seastar::future<reply_ptr> list_keys(request_ptr req, reply_ptr rep);

    seastar::future<reply_ptr> remove(request_ptr req, reply_ptr rep);

    seastar::httpd::handler_base* make_handler(endpoint fn);
    seastar::future<reply_ptr> handle(endpoint fn, request_ptr req, reply_ptr rep);

    static unsigned owner(const Key& key);

    static std::string json_escape(std::string_view in);

    Store _store;
};
