#pragma once

#include <seastar/core/sstring.hh>

#include <compare>
#include <cstddef>
#include <functional>
#include <string_view>

class Value {
public:
    explicit Value(seastar::sstring s);

    static bool is_valid(std::string_view s) noexcept;

    std::string_view view() const noexcept { return {_data.data(), _data.size()}; }
    const seastar::sstring& str() const& noexcept { return _data; }
    seastar::sstring str() && noexcept { return std::move(_data); }
    std::size_t size() const noexcept { return _data.size(); }
    bool empty() const noexcept { return _data.empty(); }

    friend bool operator==(const Value& a, const Value& b) noexcept { return a.view() == b.view(); }
    friend std::strong_ordering operator<=>(const Value& a, const Value& b) noexcept {
        return a.view() <=> b.view();
    }

private:
    seastar::sstring _data;
};

template <>
struct std::hash<Value> {
    std::size_t operator()(const Value& v) const noexcept {
        return std::hash<std::string_view>{}(v.view());
    }
};

template <>
struct fmt::formatter<Value> : fmt::formatter<std::string_view> {
    auto format(const Value& v, fmt::format_context& ctx) const {
        return fmt::formatter<std::string_view>::format(v.view(), ctx);
    }
};
