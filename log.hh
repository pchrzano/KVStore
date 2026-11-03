#pragma once

#include <seastar/util/log.hh>

// Application logger. Its level can be changed at startup, e.g.
//   --logger-log-level kvapp=debug
inline seastar::logger kvlog("kvapp");
