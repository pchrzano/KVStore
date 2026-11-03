#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

class WebApi;

// Brings every shard's Store up to date before the server starts serving.
// Run once on shard 0, after sharded<WebApi>::start().
//
// The data directory (the working directory) holds:
//   kv.meta                         format version, shard count, generation
//   store-g<generation>-<shard>.dat one data file per shard (see store.hh)
//
// - No kv.meta: a new data directory; writes kv.meta for the current shard
//   count with generation 1.
// - Same shard count: every shard replays its own file of the current
//   generation.
// - Different shard count: reshards. Every key lives in exactly one old file,
//   so each old file is replayed on its own and its live records are sent to
//   their new owner shards, which write them to files of generation + 1.
//   Only then is kv.meta switched (atomically) to the new shard count and
//   generation, and the old files deleted.
//
// Files of any generation other than the one in kv.meta are leftovers of an
// interrupted resharding and are deleted first, so a crash at any point
// leaves either the old or the new generation complete.
seastar::future<> recover(seastar::sharded<WebApi>& api);
