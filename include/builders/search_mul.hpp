#pragma once

#include "search_util.hpp"

#include "essentials.hpp"
#include "util.hpp"
#include "encoders/bit_vector.hpp"
#include "utils/hasher.hpp"

namespace pthash {

template <typename BucketsIterator, typename PilotsBuffer>
void search_sequential_mul(const uint64_t num_keys, const uint64_t num_buckets,
                           const uint64_t num_non_empty_buckets, const uint64_t seed,
                           build_configuration const& config, BucketsIterator& buckets,
                           bit_vector_builder& taken, PilotsBuffer& pilots) {
    const uint64_t max_bucket_size = (*buckets).size();
    const uint64_t table_size = taken.size();

    std::vector<uint64_t> positions;
    positions.reserve(max_bucket_size);

    search_logger log(num_keys, num_buckets);
    if (config.verbose_output) log.init();

    uint64_t processed_buckets = 0;
    for (; processed_buckets < num_non_empty_buckets; ++processed_buckets, ++buckets) {
        auto const& bucket = *buckets;
        assert(bucket.size() > 0);

        for (uint64_t pilot = 0; true; ++pilot) {
            positions.clear();

            auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
            for (; bucket_begin != bucket_end; ++bucket_begin) {
                uint64_t hash = *bucket_begin;
                uint64_t p = (__uint128_t(hash * (~pilot)) * table_size) >> 64;
                if (taken.get(p)) break;
                positions.push_back(p);
            }

            if (bucket_begin == bucket_end) {  // all keys do not have collisions with taken

                // check for in-bucket collisions
                std::sort(positions.begin(), positions.end());
                auto it = std::adjacent_find(positions.begin(), positions.end());
                if (it != positions.end())
                    continue;  // in-bucket collision detected, try next pilot

                pilots.emplace_back(bucket.id(), pilot);
                for (auto p : positions) {
                    assert(taken.get(p) == false);
                    taken.set(p, true);
                }
                if (config.verbose_output) log.update(processed_buckets, bucket.size());
                break;
            }
        }
    }

    if (config.verbose_output) log.finalize(processed_buckets);
}

template <typename BucketsIterator, typename PilotsBuffer>
void search_parallel_mul(const uint64_t num_keys, const uint64_t num_buckets,
                         const uint64_t num_non_empty_buckets, const uint64_t seed,
                         build_configuration const& config, BucketsIterator& buckets,
                         bit_vector_builder& taken, PilotsBuffer& pilots) {
    std::cerr << "search_parallel_mul not implemented" << std::endl;
    exit(1);
}

}  // namespace pthash