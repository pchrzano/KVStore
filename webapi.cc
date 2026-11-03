#include "webapi.hh"

#include "log.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/matchrules.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "openapi_spec.hh"

#include <chrono>
#include <exception>
#include <algorithm>
#include <functional>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace ss = seastar;
using ss::httpd::operation_type;

namespace {
constexpr char swagger_ui_html[] = R"html(<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>kv-app API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({ url: "/api/v1/openapi.yaml", dom_id: "#swagger-ui" });
    };
  </script>
</body>
</html>
)html";

class static_content_handler : public ss::httpd::handler_base {
    ss::sstring _content;
    ss::sstring _mime_type;

public:
    static_content_handler(ss::sstring content, ss::sstring mime_type)
        : _content(std::move(content)), _mime_type(std::move(mime_type)) {}

    ss::future<std::unique_ptr<ss::http::reply>> handle(const ss::sstring&,
            std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply> rep) override {
        rep->write_body(std::string_view(_mime_type), _content);
        return ss::make_ready_future<std::unique_ptr<ss::http::reply>>(std::move(rep));
    }
};

ss::future<std::optional<ss::sstring>> read_body(ss::input_stream<char>& in, size_t max_size) {
    std::vector<ss::temporary_buffer<char>> chunks;
    size_t total = 0;
    while (true) {
        ss::temporary_buffer<char> chunk = co_await in.read();
        if (chunk.empty()) {
            break;
        }
        total += chunk.size();
        if (total > max_size) {
            co_return std::nullopt;
        }
        chunks.push_back(std::move(chunk));
    }
    ss::sstring body(ss::sstring::initialized_later{}, total);
    char* p = body.data();
    for (const auto& chunk : chunks) {
        p = std::copy_n(chunk.get(), chunk.size(), p);
    }
    co_return body;
}

std::vector<Key> merge_sorted(std::vector<std::vector<Key>> lists) {
    size_t total = 0;
    for (const auto& l : lists) {
        total += l.size();
    }
    std::vector<Key> out;
    out.reserve(total);

    using cursor = std::pair<size_t, size_t>;
    auto greater = [&lists](const cursor& a, const cursor& b) {
        return lists[a.first][a.second] > lists[b.first][b.second];
    };
    std::priority_queue<cursor, std::vector<cursor>, decltype(greater)> heap(greater);
    for (size_t l = 0; l < lists.size(); ++l) {
        if (!lists[l].empty()) {
            heap.push({l, 0});
        }
    }
    while (!heap.empty()) {
        auto [l, i] = heap.top();
        heap.pop();
        out.push_back(std::move(lists[l][i]));
        if (i + 1 < lists[l].size()) {
            heap.push({l, i + 1});
        }
    }
    return out;
}

}

void WebApi::register_routes(ss::httpd::routes& r) {
    auto key_route = [&](operation_type op, endpoint fn) {
        auto* rule = new ss::httpd::match_rule(make_handler(fn));
        rule->add_str("/api/v1/kv").add_param("key");
        r.add(rule, op);
    };
    key_route(operation_type::PUT, &WebApi::put);
    key_route(operation_type::GET, &WebApi::get);
    key_route(operation_type::DELETE, &WebApi::remove);

    r.add(operation_type::GET, ss::httpd::url("/api/v1/kv"), make_handler(&WebApi::list_keys));

    r.add(operation_type::GET, ss::httpd::url("/api/v1/openapi.yaml"),
            new static_content_handler(openapi_spec, "application/yaml"));
    r.add(operation_type::GET, ss::httpd::url("/docs"),
            new static_content_handler(swagger_ui_html, "text/html; charset=utf-8"));
}

ss::future<WebApi::reply_ptr> WebApi::put(request_ptr req, reply_ptr rep) {
    ss::sstring key_str = req->get_path_param("key");
    if (!Key::is_valid(key_str)) {
        rep->set_status(ss::http::reply::status_type::bad_request);
        rep->write_body("json", ss::sstring(R"({"error":"key must be valid UTF-8 of at most 255 bytes"})"));
        co_return std::move(rep);
    }
    const Key key(key_str);

    std::optional<ss::sstring> body = co_await read_body(*req->content_stream, _max_value_size);
    if (!body) {
        rep->set_status(ss::http::reply::status_type::payload_too_large);
        rep->write_body("json", ss::sstring(fmt::format(R"({{"error":"value is larger than {} bytes"}})",
                _max_value_size)));
        co_return std::move(rep);
    }
    if (!Value::is_valid(*body)) {
        rep->set_status(ss::http::reply::status_type::bad_request);
        rep->write_body("json", ss::sstring(R"({"error":"value must be valid UTF-8"})"));
        co_return std::move(rep);
    }
    Value value(std::move(*body));

    bool created = co_await container().invoke_on(owner(key),
            [&key, value = std::move(value)](WebApi& api) mutable {
                return api._store.put(key, std::move(value));
            });

    rep->set_status(created ? ss::http::reply::status_type::created
                            : ss::http::reply::status_type::ok);
    rep->write_body("json", ss::sstring(fmt::format(R"({{"key":"{}","created":{}}})",
            json_escape(key.view()), created)));
    co_return std::move(rep);
}

ss::future<WebApi::reply_ptr> WebApi::get(request_ptr req, reply_ptr rep) {
    ss::sstring key_str = req->get_path_param("key");
    if (!Key::is_valid(key_str)) {
        rep->set_status(ss::http::reply::status_type::bad_request);
        rep->write_body("json", ss::sstring(R"({"error":"key must be valid UTF-8 of at most 255 bytes"})"));
        co_return std::move(rep);
    }
    const Key key(key_str);

    std::optional<Value> value = co_await container().invoke_on(owner(key),
            [&key](WebApi& api) {
                return api._store.get(key);
            });

    if (!value) {
        rep->set_status(ss::http::reply::status_type::not_found);
        rep->write_body("json", ss::sstring(R"({"error":"key not found"})"));
        co_return std::move(rep);
    }
    rep->write_body("json", ss::sstring(fmt::format(R"({{"key":"{}","value":"{}"}})",
            json_escape(key.view()), json_escape(value->view()))));
    co_return std::move(rep);
}

ss::future<WebApi::reply_ptr> WebApi::list_keys(request_ptr req, reply_ptr rep) {
    std::vector<std::vector<Key>> per_shard = co_await container().map([](WebApi& api) {
        return api._store.keys();
    });
    std::vector<Key> keys = merge_sorted(std::move(per_shard));

    std::string body = R"({"keys":[)";
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i > 0) {
            body += ',';
        }
        body += '"';
        body += json_escape(keys[i].view());
        body += '"';
    }
    body += "]}";
    rep->write_body("json", ss::sstring(body));
    co_return std::move(rep);
}

ss::future<WebApi::reply_ptr> WebApi::remove(request_ptr req, reply_ptr rep) {
    ss::sstring key_str = req->get_path_param("key");
    if (!Key::is_valid(key_str)) {
        rep->set_status(ss::http::reply::status_type::bad_request);
        rep->write_body("json", ss::sstring(R"({"error":"key must be valid UTF-8 of at most 255 bytes"})"));
        co_return std::move(rep);
    }
    const Key key(key_str);

    bool deleted = co_await container().invoke_on(owner(key),
            [&key](WebApi& api) {
                return api._store.remove(key);
            });

    if (!deleted) {
        rep->set_status(ss::http::reply::status_type::not_found);
        rep->write_body("json", ss::sstring(R"({"error":"key not found"})"));
        co_return std::move(rep);
    }
    rep->set_status(ss::http::reply::status_type::no_content);
    co_return std::move(rep);
}

ss::httpd::handler_base* WebApi::make_handler(endpoint fn) {
    return new ss::httpd::function_handler(
            [this, fn](request_ptr req, reply_ptr rep) { return handle(fn, std::move(req), std::move(rep)); },
            "json");
}

ss::future<WebApi::reply_ptr> WebApi::handle(endpoint fn, request_ptr req, reply_ptr rep) {
    const auto started = std::chrono::steady_clock::now();
    const ss::sstring method = req->_method;
    const ss::sstring url = req->_url;
    std::string route = fmt::format("shard {}", ss::this_shard_id());
    if (req->param.exists("key")) {
        const ss::sstring key = req->get_path_param("key");
        if (Key::is_valid(key)) {
            route += fmt::format(" → owner {}", owner(Key(key)));
        }
    }
    std::exception_ptr ex;
    try {
        ss::future<reply_ptr> result = (this->*fn)(std::move(req), std::move(rep));
        rep = co_await std::move(result);
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        httplog.error("{} {} → 500 in {:.1f} ms ({}): {}", method, url, elapsed_ms(started), route,
                ss::formattable(ex));
        std::rethrow_exception(ex);
    }
    httplog.debug("{} {} → {} in {:.1f} ms ({})", method, url, static_cast<int>(rep->_status),
            elapsed_ms(started), route);
    co_return std::move(rep);
}

unsigned WebApi::owner(const Key& key) {
    return Store::owner(key, ss::this_smp_shard_count());
}

std::string WebApi::json_escape(std::string_view in) {
    std::string out;
    for (unsigned char c : in) {
        switch (c) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (c < 0x20) {
                out += fmt::format("\\u{:04x}", c);
            } else {
                out += static_cast<char>(c);
            }
        }
    }
    return out;
}
