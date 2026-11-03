#include "lru_cache.hh"

#include <utility>

LRUCache::LRUCache(std::size_t capacity)
    : _capacity(capacity) {}

const Value* LRUCache::get(const Key& key) {
    auto found = _index.find(key);
    if (found == _index.end()) {
        return nullptr;
    }
    // Move the node to the front; iterators stay valid.
    _lru.splice(_lru.begin(), _lru, found->second);
    return &found->second->value;
}

void LRUCache::put(const Key& key, Value value) {
    if (_capacity == 0) {
        return;
    }
    if (auto found = _index.find(key); found != _index.end()) {
        auto it = found->second;
        it->value = std::move(value);
        _lru.splice(_lru.begin(), _lru, it);
        return;
    }
    _lru.push_front(Entry{key, std::move(value)});
    _index.emplace(key, _lru.begin());
    if (_index.size() > _capacity) {
        _index.erase(_lru.back().key);
        _lru.pop_back();
    }
}
