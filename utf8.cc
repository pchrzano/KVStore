#include "utf8.hh"

namespace utf8 {
bool is_valid(std::string_view s) noexcept {
    auto p = reinterpret_cast<const unsigned char*>(s.data());
    const auto end = p + s.size();
    while (p < end) {
        const unsigned char c = *p;
        if (c < 0x80) {
            ++p;
            continue;
        }

        int len;
        char32_t cp;
        char32_t min;
        if ((c & 0xE0) == 0xC0) {
            len = 2; cp = c & 0x1F; min = 0x80;
        } else if ((c & 0xF0) == 0xE0) {
            len = 3; cp = c & 0x0F; min = 0x800;
        } else if ((c & 0xF8) == 0xF0) {
            len = 4; cp = c & 0x07; min = 0x10000;
        } else {
            return false;
        }
        if (end - p < len) {
            return false;
        }
        for (int i = 1; i < len; ++i) {
            if ((p[i] & 0xC0) != 0x80) {
                return false;
            }
            cp = (cp << 6) | (p[i] & 0x3F);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
            return false;
        }
        p += len;
    }
    return true;
}

}
