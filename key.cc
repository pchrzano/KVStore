#include "key.hh"

#include "utf8.hh"

#include <fmt/format.h>

#include <algorithm>
#include <stdexcept>

static_assert(sizeof(Key) == 256);

Key::Key(std::string_view s) {
    if (s.size() > max_size) {
        throw std::invalid_argument(fmt::format("key is {} bytes, max is {}", s.size(), max_size));
    }
    if (!utf8::is_valid(s)) {
        throw std::invalid_argument("key is not valid UTF-8");
    }
    std::ranges::copy(s, _data.begin());
    _size = static_cast<std::uint8_t>(s.size());
}

bool Key::is_valid(std::string_view s) noexcept {
    return s.size() <= max_size && utf8::is_valid(s);
}
