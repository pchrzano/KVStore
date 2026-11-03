#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include "key.hh"
#include "lru_cache.hh"
#include "value.hh"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

// One shard's part of the key-value data.
//
// Every shard has its own instance, which only that shard's thread touches,
// so there are no locks. Which shard owns a key is decided by owner().
//
// Keys are kept in a std::map, so they are always in sorted order.
//
// Data lives in store-g<generation>-<shard>.dat, which is append-only. Each
// record is (integers little-endian):
//   [type: u8][key size: u64][key][value size: u64][value]
// A delete record has value size 0 and no value. Replaying a file in order
// gives the current state: the last record for a key wins. The generation
// changes when the data is resharded (see recovery.hh).
//
// The most recently used values are kept in an LRUCache, so a get of a hot
// key needs no disk read. The index is always checked first, so a deleted
// key left in the cache is never returned.
class Store {
public:
    // `cache_capacity`: how many key-value pairs this shard's cache holds.
    explicit Store(std::size_t cache_capacity = LRUCache::default_capacity)
        : _cache(cache_capacity) {}

    enum class RecordType : uint8_t {
        put = 1,
        del = 2,
    };

    // The shard that owns `key` when there are `shard_count` shards. Uses a
    // stable hash (not std::hash), so it stays the same across builds and
    // restarts - the data files depend on it.
    static unsigned owner(const Key& key, unsigned shard_count);

    // Data file name for a generation and shard.
    static seastar::sstring file_name(uint64_t generation, unsigned shard);

    // --- Startup ---------------------------------------------------------

    // Rebuilds the index from this shard's file of `generation`. An
    // incomplete or invalid tail (e.g. from a crash during an append) is
    // truncated away.
    seastar::future<> load(uint64_t generation);

    // Starts empty, writing to `generation` (new data directory, or resharding).
    void start_generation(uint64_t generation);

    // Appends many put records in one write and one fsync (used by
    // resharding). Keys must not already exist on this shard.
    seastar::future<> import(std::vector<std::pair<Key, Value>> records);

    struct ScanResult {
        std::map<Key, uint64_t> live; // key -> offset of its latest put record
        uint64_t records = 0;         // valid records read
        uint64_t valid_end = 0;       // end of the last valid record
        uint64_t file_size = 0;
    };
    // Replays a data file. Stops at the first incomplete or invalid record.
    static seastar::future<ScanResult> scan(seastar::sstring path);

    // Reads the value of the put record for `key` at `offset` in an open file.
    static seastar::future<Value> read_value(seastar::file& f, const seastar::sstring& path,
            uint64_t offset, const Key& key);

    // --- Operations --------------------------------------------------------
    // Coroutines take their arguments by value, so callers may pass
    // temporaries without keeping them alive.

    // Appends a put record and remembers its offset. Inserts the key or
    // updates it. Resolves to true if the key was created.
    seastar::future<bool> put(Key key, Value value);

    // Returns the value from the cache, or reads the key's latest record
    // from the data file and caches it. Resolves to the value, or nullopt
    // if the key doesn't exist.
    seastar::future<std::optional<Value>> get(Key key) const;

    // Appends a delete record (tombstone) and forgets the key.
    // Resolves to true if the key existed.
    seastar::future<bool> remove(Key key);

    // All keys on this shard, sorted.
    std::vector<Key> keys() const;

    std::size_t size() const noexcept { return _data.size(); }

    seastar::future<> stop() { return seastar::make_ready_future<>(); }

private:
    struct Record {
        RecordType type;
        const Key* key;
        std::string_view value;
    };

    // Appends records to the data file in one write and one fsync; resolves
    // to each record's offset. The caller must hold _append_lock.
    seastar::future<std::vector<uint64_t>> append_records(const std::vector<Record>& records);
    static uint64_t header_size(const Key& key);
    seastar::sstring file_name() const;

    std::map<Key, uint64_t> _data; // key -> record offset in the data file
    mutable LRUCache _cache;       // recently used values; filled by get() too
    uint64_t _generation = 0;
    seastar::semaphore _append_lock{1}; // one append at a time per shard
    bool _dir_synced = false;           // data file's directory entry fsynced
};
