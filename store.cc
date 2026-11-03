#include "store.hh"

#include "log.hh"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <string_view>
#include <sys/uio.h>
#include <utility>
#include <vector>

unsigned Store::owner(const Key& key, unsigned shard_count) {
    // FNV-1a, then a murmur3 finalizer so the low bits mix well for `%`.
    uint64_t h = 14695981039346656037ull;
    for (unsigned char c : key.view()) {
        h = (h ^ c) * 1099511628211ull;
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdull;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ull;
    h ^= h >> 33;
    return h % shard_count;
}

seastar::sstring Store::file_name(uint64_t generation, unsigned shard) {
    return seastar::format("store-g{}-{}.dat", generation, shard);
}

seastar::sstring Store::file_name() const {
    return file_name(_generation, seastar::this_shard_id());
}

// [type: u8][key size: u64][key][value size: u64]
uint64_t Store::header_size(const Key& key) {
    return 1 + sizeof(uint64_t) + key.size() + sizeof(uint64_t);
}

// --- Startup ---------------------------------------------------------------

void Store::start_generation(uint64_t generation) {
    _generation = generation;
    _data.clear();
    _dir_synced = false;
}

seastar::future<> Store::load(uint64_t generation) {
    start_generation(generation);
    const seastar::sstring path = file_name();
    if (!co_await seastar::file_exists(path)) {
        co_return;
    }
    ScanResult r = co_await scan(path);

    const unsigned shard = seastar::this_shard_id();
    const unsigned count = seastar::this_smp_shard_count();
    for (const auto& [key, offset] : r.live) {
        if (owner(key, count) != shard) {
            throw std::runtime_error(seastar::format(
                    "{}: key {} at offset {} belongs to shard {}, not {}",
                    path, key, offset, owner(key, count), shard));
        }
    }

    if (r.valid_end < r.file_size) {
        kvlog.warn("{}: dropping {} bytes of incomplete or invalid data at offset {}",
                path, r.file_size - r.valid_end, r.valid_end);
        seastar::file f = co_await seastar::open_file_dma(path, seastar::open_flags::rw);
        std::exception_ptr ex;
        try {
            co_await f.truncate(r.valid_end);
            co_await f.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await f.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    _data = std::move(r.live);
    kvlog.info("{}: loaded {} keys from {} records", path, _data.size(), r.records);
}

seastar::future<> Store::import(std::vector<std::pair<Key, Value>> records) {
    std::vector<Record> recs;
    recs.reserve(records.size());
    for (const auto& [key, value] : records) {
        recs.push_back({RecordType::put, &key, value.view()});
    }
    auto units = co_await seastar::get_units(_append_lock, 1);
    std::vector<uint64_t> offsets = co_await append_records(recs);
    for (size_t i = 0; i < records.size(); ++i) {
        _data.insert_or_assign(records[i].first, offsets[i]);
    }
}

seastar::future<Store::ScanResult> Store::scan(seastar::sstring path) {
    ScanResult r;
    seastar::file f = co_await seastar::open_file_dma(path, seastar::open_flags::ro);
    r.file_size = co_await f.size();
    // Sequential reads with read-ahead; closing the stream closes the file.
    auto in = seastar::make_file_input_stream(f, 0, r.file_size,
            seastar::file_input_stream_options{.buffer_size = 128 * 1024, .read_ahead = 4});
    std::exception_ptr ex;
    try {
        uint64_t pos = 0;
        while (pos < r.file_size) {
            auto head = co_await in.read_exactly(1 + sizeof(uint64_t));
            if (head.size() < 1 + sizeof(uint64_t)) {
                break;
            }
            const auto type = static_cast<RecordType>(head[0]);
            const uint64_t key_size = seastar::read_le<uint64_t>(head.get() + 1);
            if ((type != RecordType::put && type != RecordType::del) || key_size > Key::max_size) {
                break;
            }
            auto key_buf = co_await in.read_exactly(key_size);
            const std::string_view key_bytes(key_buf.get(), key_buf.size());
            if (key_buf.size() < key_size || !Key::is_valid(key_bytes)) {
                break;
            }
            auto size_buf = co_await in.read_exactly(sizeof(uint64_t));
            if (size_buf.size() < sizeof(uint64_t)) {
                break;
            }
            const uint64_t value_size = seastar::read_le<uint64_t>(size_buf.get());
            const uint64_t end = pos + 1 + 2 * sizeof(uint64_t) + key_size + value_size;
            if ((type == RecordType::del && value_size != 0) || end > r.file_size) {
                break; // torn or corrupt
            }
            co_await in.skip(value_size); // the index only needs offsets

            Key key(key_bytes);
            if (type == RecordType::put) {
                r.live.insert_or_assign(std::move(key), pos);
            } else {
                r.live.erase(key);
            }
            ++r.records;
            pos = end;
        }
        r.valid_end = pos;
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return r;
}

seastar::future<Value> Store::read_value(seastar::file& f, const seastar::sstring& path,
        uint64_t offset, const Key& key) {
    const uint64_t header_len = header_size(key);
    // Returns at least the header, and data up to the next block boundary,
    // so a small value usually comes back in this same read.
    auto head = co_await f.dma_read_bulk<char>(offset, header_len);
    if (head.size() < header_len) {
        throw std::runtime_error(seastar::format("{}: truncated record at {}", path, offset));
    }
    const char* p = head.get();
    // The index only points at put records, so anything else means corruption.
    if (static_cast<RecordType>(p[0]) != RecordType::put) {
        throw std::runtime_error(seastar::format("{}: record at {} is not a put", path, offset));
    }
    if (seastar::read_le<uint64_t>(p + 1) != key.size()
            || std::string_view(p + 1 + sizeof(uint64_t), key.size()) != key.view()) {
        throw std::runtime_error(seastar::format("{}: record at {} is not for key {}", path, offset, key));
    }
    const uint64_t value_size = seastar::read_le<uint64_t>(p + 1 + sizeof(uint64_t) + key.size());

    seastar::sstring value(seastar::sstring::initialized_later{}, value_size);
    const uint64_t in_head = std::min<uint64_t>(value_size, head.size() - header_len);
    std::copy_n(p + header_len, in_head, value.data());
    if (in_head < value_size) {
        auto rest = co_await f.dma_read_exactly<char>(offset + header_len + in_head, value_size - in_head);
        std::copy_n(rest.get(), rest.size(), value.data() + in_head);
    }
    co_return Value(std::move(value)); // Value re-checks UTF-8, catching corrupt data
}

// --- Operations --------------------------------------------------------------

seastar::future<bool> Store::put(Key key, Value value) {
    kvlog.info("Putting key: {}, value: {} at shard: {}", key, value, seastar::this_shard_id());
    auto units = co_await seastar::get_units(_append_lock, 1);
    std::vector<uint64_t> offsets = co_await append_records({{RecordType::put, &key, value.view()}});
    auto [it, inserted] = _data.insert_or_assign(key, offsets[0]);
    _cache.put(key, std::move(value)); // only once it is on disk
    co_return inserted;
}

seastar::future<std::optional<Value>> Store::get(Key key) const {
    auto it = _data.find(key);
    if (it == _data.end()) {
        co_return std::nullopt;
    }
    if (const Value* cached = _cache.get(key)) {
        co_return *cached;
    }
    const uint64_t offset = it->second;
    const seastar::sstring path = file_name();

    seastar::file f = co_await seastar::open_file_dma(path, seastar::open_flags::ro);
    std::exception_ptr ex;
    std::optional<Value> result;
    try {
        result.emplace(co_await read_value(f, path, offset, key));
    } catch (...) {
        ex = std::current_exception();
    }
    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    // While we were reading, a put or delete may have changed the key. Only
    // cache the value if it is still the latest one (a new put always gets
    // a new offset).
    if (auto now = _data.find(key); now != _data.end() && now->second == offset) {
        _cache.put(key, *result);
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
    co_await append_records({{RecordType::del, &key, {}}});
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

// Seastar file I/O is DMA (O_DIRECT): writes must start at an aligned offset
// and cover whole blocks. So one append is: read the file's last partial
// block (if any), put the records after it, write everything from that
// block's start in a single dma_write, truncate the padding off the end, and
// fsync.
seastar::future<std::vector<uint64_t>> Store::append_records(const std::vector<Record>& records) {
    const seastar::sstring path = file_name();
    seastar::file f = co_await seastar::open_file_dma(path,
            seastar::open_flags::rw | seastar::open_flags::create);
    std::exception_ptr ex;
    std::vector<uint64_t> offsets;
    offsets.reserve(records.size());
    try {
        const uint64_t start = co_await f.size();
        uint64_t records_len = 0;
        for (const auto& r : records) {
            offsets.push_back(start + records_len);
            records_len += header_size(*r.key) + r.value.size();
        }

        const uint64_t align = f.disk_write_dma_alignment();
        const uint64_t block_start = seastar::align_down(start, align);
        const uint64_t tail = start - block_start; // bytes already in the last block
        const uint64_t buf_len = seastar::align_up(tail + records_len, align);
        auto buf = seastar::allocate_aligned_buffer<char>(buf_len,
                std::max<uint64_t>(align, f.memory_dma_alignment()));

        if (tail > 0) {
            size_t n = co_await f.dma_read(block_start, std::vector<iovec>{{buf.get(), align}});
            if (n < tail) {
                throw std::runtime_error(seastar::format("{}: short read of last block", path));
            }
        }

        char* p = buf.get() + tail;
        for (const auto& r : records) {
            *p++ = static_cast<char>(r.type);
            seastar::write_le<uint64_t>(p, r.key->size());
            p = std::ranges::copy(r.key->view(), p + sizeof(uint64_t)).out;
            seastar::write_le<uint64_t>(p, r.value.size());
            p = std::ranges::copy(r.value, p + sizeof(uint64_t)).out;
        }
        std::fill(p, buf.get() + buf_len, 0);

        size_t written = co_await f.dma_write(block_start, buf.get(), buf_len);
        if (written != buf_len) {
            throw std::runtime_error(seastar::format("{}: short write", path));
        }
        co_await f.truncate(start + records_len);
        // fdatasync: the records and the new file size are on stable storage
        // before the caller reports success.
        co_await f.flush();
        if (!_dir_synced) {
            // The file may have just been created; make its directory entry
            // durable too. Once per shard per generation is enough.
            co_await seastar::sync_directory(".");
            _dir_synced = true;
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return offsets;
}
