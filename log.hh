#pragma once

#include <seastar/util/log.hh>

#include <chrono>

inline seastar::logger kvlog("kvapp");
inline seastar::logger loaderlog("kv-loader");
inline seastar::logger storelog("kv-store");
inline seastar::logger httplog("kv-http");

inline double elapsed_ms(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}
