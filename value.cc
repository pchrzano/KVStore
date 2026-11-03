#include "value.hh"

#include "utf8.hh"

#include <stdexcept>
#include <utility>

Value::Value(seastar::sstring s) : _data(std::move(s)) {
    if (!utf8::is_valid(view())) {
        throw std::invalid_argument("value is not valid UTF-8");
    }
}

bool Value::is_valid(std::string_view s) noexcept {
    return utf8::is_valid(s);
}
