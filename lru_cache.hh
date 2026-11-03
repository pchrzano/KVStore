#pragma once

#include "key.hh"
#include "value.hh"

#include <cstddef>
#include <list>
#include <unordered_map>

class LRUCache {
public:
    static constexpr std::size_t default_capacity = 100;

    explicit LRUCache(std::size_t capacity = default_capacity);

    const Value* get(const Key& key);

    void put(const Key& key, Value value);

    std::size_t size() const noexcept { return _index.size(); }
    std::size_t capacity() const noexcept { return _capacity; }

private:
    struct Entry {
        Key key;
        Value value;
    };
    using List = std::list<Entry>;

    List _lru;
    std::unordered_map<Key, List::iterator> _index;
    std::size_t _capacity;
};
