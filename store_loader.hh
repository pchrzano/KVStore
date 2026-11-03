#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

class WebApi;

class StoreLoader {
public:
    explicit StoreLoader(seastar::sharded<WebApi>& api) noexcept : _api(api) {}

    seastar::future<> load_or_reshard();

private:
    struct Meta {
        unsigned shard_count;
        uint64_t generation;
    };

    static constexpr char meta_file[] = "kv.meta";
    static constexpr char meta_tmp_file[] = "kv.meta.tmp";
    static constexpr unsigned format_version = 1;
    static constexpr std::size_t batch_bytes = 1 << 20;
    static constexpr std::size_t batch_records = 4096;

    seastar::future<> reshard(Meta old, unsigned new_count);

    static seastar::future<std::size_t> migrate_file(seastar::sharded<WebApi>& api, seastar::sstring path,
            unsigned new_count);
    static seastar::future<std::optional<Meta>> read_meta();
    static seastar::future<> write_meta(Meta m);
    static seastar::future<> remove_other_generations(uint64_t keep, bool replaced);
    static std::optional<uint64_t> data_file_generation(std::string_view name);
    static std::optional<uint64_t> parse_uint(std::string_view s);

    seastar::sharded<WebApi>& _api;
    seastar::sstring _stage;
};
