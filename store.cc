#include "store.hh"

#include "log.hh"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/format.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <sys/uio.h>
#include <utility>
#include <vector>

seastar::future<bool> Store::put(Key key, Value value) {
    kvlog.info("Putting key: {}, value: {} at shard: {}", key, value, seastar::this_shard_id());
    auto units = co_await seastar::get_units(_append_lock, 1);
    uint64_t offset = co_await append_record(RecordType::put, key, value.view());
    auto [it, inserted] = _data.insert_or_assign(key, offset);
    co_return inserted;
}

seastar::future<std::optional<Value>> Store::get(Key key) const {
    auto it = _data.find(key);
    if (it == _data.end()) {
        co_return std::nullopt;
    }
    const uint64_t offset = it->second;
    const uint64_t header_len = header_size(key);

    seastar::file f = co_await seastar::open_file_dma(file_name(), seastar::open_flags::ro);
    std::exception_ptr ex;
    std::optional<Value> result;
    try {
        // Returns at least the header, and data up to the next block boundary,
        // so a small value usually comes back in this same read.
        auto head = co_await f.dma_read_bulk<char>(offset, header_len);
        if (head.size() < header_len) {
            throw std::runtime_error(seastar::format("{}: truncated record at {}", file_name(), offset));
        }
        const char* p = head.get();
        // _data only points at put records, so anything else means corruption.
        if (static_cast<RecordType>(p[0]) != RecordType::put) {
            throw std::runtime_error(seastar::format("{}: record at {} is not a put", file_name(), offset));
        }
        p += 1;
        if (seastar::read_le<uint64_t>(p) != key.size()
                || std::string_view(p + sizeof(uint64_t), key.size()) != key.view()) {
            throw std::runtime_error(seastar::format("{}: record at {} is not for key {}", file_name(), offset, key));
        }
        const uint64_t value_size = seastar::read_le<uint64_t>(p + sizeof(uint64_t) + key.size());
        p = head.get();

        seastar::sstring value(seastar::sstring::initialized_later{}, value_size);
        const uint64_t in_head = std::min<uint64_t>(value_size, head.size() - header_len);
        std::copy_n(p + header_len, in_head, value.data());
        if (in_head < value_size) {
            auto rest = co_await f.dma_read_exactly<char>(offset + header_len + in_head, value_size - in_head);
            std::copy_n(rest.get(), rest.size(), value.data() + in_head);
        }
        result.emplace(std::move(value)); // Value re-checks UTF-8, catching corrupt data
    } catch (...) {
        ex = std::current_exception();
    }
    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return result;
}

seastar::future<bool> Store::remove(Key key) {
    if (!_data.contains(key)) {
        co_return false; // nothing to delete, no disk write
    }
    auto units = co_await seastar::get_units(_append_lock, 1);
    // Re-check: another delete may have removed the key while we waited.
    if (!_data.contains(key)) {
        co_return false;
    }
    co_await append_record(RecordType::del, key, {});
    _data.erase(key); // only after the tombstone is written
    co_return true;
}

std::vector<Key> Store::keys() const {
    std::vector<Key> result;
    result.reserve(_data.size());
    for (const auto& [key, value] : _data) {
        result.push_back(key);
    }
    return result;
}

// [type: u8][key size: u64][key][value size: u64]
uint64_t Store::header_size(const Key& key) {
    return 1 + sizeof(uint64_t) + key.size() + sizeof(uint64_t);
}

seastar::sstring Store::file_name() {
    return seastar::format("store-{}.dat", seastar::this_shard_id());
}

// Seastar file I/O is DMA (O_DIRECT): writes must start at an aligned offset
// and cover whole blocks. So one append is: read the file's last partial
// block (if any), put the record after it, write everything from that block's
// start in a single dma_write, truncate the padding off the end, and fsync.
seastar::future<uint64_t> Store::append_record(RecordType type, const Key& key, std::string_view value) {
    seastar::file f = co_await seastar::open_file_dma(file_name(),
            seastar::open_flags::rw | seastar::open_flags::create);
    std::exception_ptr ex;
    uint64_t offset = 0;
    try {
        offset = co_await f.size();
        const uint64_t align = f.disk_write_dma_alignment();
        const uint64_t block_start = seastar::align_down(offset, align);
        const uint64_t tail = offset - block_start; // bytes already in the last block
        const uint64_t record_len = header_size(key) + value.size();
        const uint64_t buf_len = seastar::align_up(tail + record_len, align);
        auto buf = seastar::allocate_aligned_buffer<char>(buf_len,
                std::max<uint64_t>(align, f.memory_dma_alignment()));

        if (tail > 0) {
            size_t n = co_await f.dma_read(block_start, std::vector<iovec>{{buf.get(), align}});
            if (n < tail) {
                throw std::runtime_error(seastar::format("{}: short read of last block", file_name()));
            }
        }

        char* p = buf.get() + tail;
        *p++ = static_cast<char>(type);
        seastar::write_le<uint64_t>(p, key.size());
        p = std::ranges::copy(key.view(), p + sizeof(uint64_t)).out;
        seastar::write_le<uint64_t>(p, value.size());
        p = std::ranges::copy(value, p + sizeof(uint64_t)).out;
        std::fill(p, buf.get() + buf_len, 0);

        size_t written = co_await f.dma_write(block_start, buf.get(), buf_len);
        if (written != buf_len) {
            throw std::runtime_error(seastar::format("{}: short write", file_name()));
        }
        co_await f.truncate(offset + record_len);
        // fdatasync: the record and the new file size are on stable storage
        // before put/remove report success.
        co_await f.flush();
        if (!_dir_synced) {
            // The file may have just been created; make its directory entry
            // durable too. Once per shard per process is enough.
            seastar::file dir = co_await seastar::open_directory(".");
            co_await dir.flush();
            co_await dir.close();
            _dir_synced = true;
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return offset;
}
