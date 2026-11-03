#pragma once

#include <string_view>

namespace utf8 {

// True if `s` is well-formed UTF-8. Rejects truncated sequences, stray
// continuation bytes, overlong encodings, surrogates (U+D800..U+DFFF) and
// code points above U+10FFFF.
bool is_valid(std::string_view s) noexcept;

} // namespace utf8
