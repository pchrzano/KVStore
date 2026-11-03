#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include "key.hh"
#include "value.hh"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string_view>
#include <vector>

// One shard's part of the key-value data.
//
// Meant to be used as seastar::sharded<Store>: every shard has its own
// instance, which only that shard's thread touches, so there are no locks.
// Which shard owns a key is decided by the caller (e.g. with invoke_on).
//
// Keys are kept in a std::map, so they are always in sorted order.
//
// store-<shard>.dat is append-only. Each record is (integers little-endian):
//   [type: u8][key size: u64][key][value size: u64][value]
// A delete record has value size 0 and no value. Replaying a file in order
// gives the current state: the last record for a key wins.
class Store {
public:
    enum class RecordType : uint8_t {
        put = 1,
        del = 2,
    };

    // Appends a put record to store-<shard>.dat and remembers its offset.
    // Inserts the key or updates it. Resolves to true if the key was created.
    // Coroutines take their arguments by value, so callers may pass
    // temporaries without keeping them alive.
    seastar::future<bool> put(Key key, Value value);

    // Reads the key's latest record from store-<shard>.dat. Resolves to the
    // value, or nullopt if the key doesn't exist.
    seastar::future<std::optional<Value>> get(Key key) const;

    // Appends a delete record (tombstone) and forgets the key.
    // Resolves to true if the key existed.
    seastar::future<bool> remove(Key key);

    // All keys on this shard, sorted.
    std::vector<Key> keys() const;

    std::size_t size() const noexcept { return _data.size(); }

    // Required by seastar::sharded<>.
    seastar::future<> stop() { return seastar::make_ready_future<>(); }

private:
    // Appends one record to the data file; resolves to the record's offset.
    // The caller must hold _append_lock.
    seastar::future<uint64_t> append_record(RecordType type, const Key& key, std::string_view value);
    static uint64_t header_size(const Key& key);
    static seastar::sstring file_name();

    std::map<Key, uint64_t> _data; // key -> record offset in the data file
    seastar::semaphore _append_lock{1}; // one append at a time per shard
    bool _dir_synced = false;           // data file's directory entry fsynced

};
