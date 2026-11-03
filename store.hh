#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include "key.hh"
#include "lru_cache.hh"
#include "value.hh"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

class Store {
public:
    explicit Store(std::size_t cache_capacity = LRUCache::default_capacity)
        : _cache(cache_capacity) {}

    enum class RecordType : uint8_t {
        put = 1,
        del = 2,
    };

    static unsigned owner(const Key& key, unsigned shard_count);

    static seastar::sstring file_name(uint64_t generation, unsigned shard);

    struct LoadStats {
        std::size_t keys = 0;
        uint64_t records = 0;
        uint64_t bytes = 0;

        friend LoadStats operator+(LoadStats a, const LoadStats& b) noexcept {
            return {a.keys + b.keys, a.records + b.records, a.bytes + b.bytes};
        }
    };

    seastar::future<LoadStats> load(uint64_t generation);

    void start_generation(uint64_t generation);

    seastar::future<> import(std::vector<std::pair<Key, Value>> records);

    struct ScanResult {
        std::map<Key, uint64_t> live;
        uint64_t records = 0;
        uint64_t valid_end = 0;
        uint64_t file_size = 0;
    };
    static seastar::future<ScanResult> scan(seastar::sstring path);

    static seastar::future<Value> read_value(seastar::file& f, const seastar::sstring& path,
            uint64_t offset, const Key& key);

    seastar::future<bool> put(Key key, Value value);

    seastar::future<std::optional<Value>> get(Key key) const;

    seastar::future<bool> remove(Key key);

    std::vector<Key> keys() const;

    std::size_t size() const noexcept { return _data.size(); }

    seastar::future<> stop() { return seastar::make_ready_future<>(); }

private:
    struct Record {
        RecordType type;
        const Key* key;
        std::string_view value;
    };

    static constexpr std::chrono::milliseconds slow_append_threshold{100};

    seastar::future<std::vector<uint64_t>> append_records(const std::vector<Record>& records);
    static uint64_t header_size(const Key& key);
    seastar::sstring file_name() const;

    std::map<Key, uint64_t> _data;
    mutable LRUCache _cache;
    uint64_t _generation = 0;
    seastar::semaphore _append_lock{1};
    bool _dir_synced = false;
};
