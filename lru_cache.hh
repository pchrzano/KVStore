#pragma once

#include "key.hh"
#include "value.hh"

#include <cstddef>
#include <list>
#include <unordered_map>

// A least recently used cache of key -> value pairs, holding at most
// `capacity` pairs regardless of their size. When a put goes over the
// capacity, the least recently used pair is evicted.
//
// Not thread-safe: meant to be owned by one shard, like Store.
class LRUCache {
public:
    static constexpr std::size_t default_capacity = 100;

    explicit LRUCache(std::size_t capacity = default_capacity);

    // The cached value, or nullptr on a miss. A hit marks the pair as most
    // recently used. The pointer is valid until the next put().
    const Value* get(const Key& key);

    // Inserts or replaces the value for `key` and marks it most recently
    // used. Evicts the least recently used pair if the cache is over capacity.
    void put(const Key& key, Value value);

    std::size_t size() const noexcept { return _index.size(); }
    std::size_t capacity() const noexcept { return _capacity; }

private:
    struct Entry {
        Key key;
        Value value;
    };
    using List = std::list<Entry>;

    List _lru; // front = most recently used
    std::unordered_map<Key, List::iterator> _index;
    std::size_t _capacity;
};
