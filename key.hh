#pragma once

#include <seastar/core/sstring.hh>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

class Key {
public:
    static constexpr std::size_t max_size = 255;

    explicit Key(std::string_view s);

    static bool is_valid(std::string_view s) noexcept;

    std::string_view view() const noexcept { return {_data.data(), _size}; }
    seastar::sstring str() const { return seastar::sstring(_data.data(), _size); }
    std::size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }

    friend bool operator==(const Key& a, const Key& b) noexcept { return a.view() == b.view(); }
    friend std::strong_ordering operator<=>(const Key& a, const Key& b) noexcept {
        return a.view() <=> b.view();
    }

private:
    std::array<char, max_size> _data{};
    std::uint8_t _size = 0;
};

template <>
struct std::hash<Key> {
    std::size_t operator()(const Key& k) const noexcept {
        return std::hash<std::string_view>{}(k.view());
    }
};

template <>
struct fmt::formatter<Key> : fmt::formatter<std::string_view> {
    auto format(const Key& k, fmt::format_context& ctx) const {
        return fmt::formatter<std::string_view>::format(k.view(), ctx);
    }
};
