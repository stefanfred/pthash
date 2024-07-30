#include <iostream>

#include "pthash.hpp"
#include "util.hpp"  // for functions distinct_keys and check

int main() {
    using namespace pthash;

    /* Generate 1M random 64-bit keys as input data. */
    static const uint64_t num_keys = 10000000;
    static const uint64_t seed = 1234567890;
    std::cout << "generating input data..." << std::endl;
    std::vector<uint64_t> keys = distinct_keys<uint64_t>(num_keys, default_hash64(seed, seed));
    assert(keys.size() == num_keys);

    /* Set up a build configuration. */
    build_configuration config;
    config.seed = seed;
    config.lambda = 5;
    config.alpha = 1;
    config.search = pthash_search_type::mult_hash;
    config.dense_partitioning = true;
    config.avg_partition_size = 3000;
    config.minimal_output = true;
    config.verbose_output = true;

    /* Declare the PTHash function. */
    // typedef single_phf<
    //     murmurhash2_64,                                                       // base hasher
    //     skew_bucketer,                                                        // bucketer type
    //     dictionary_dictionary,                                                // encoder type
    //     true,                                                                 // minimal
    //     pthash_search_type::add_displacement                                  // xor displacement
    //                                                                           // search
    //     >
    typedef dense_partitioned_phf<xxhash128,                       // base hasher
                                  opt_bucketer,                         // bucketer type
                                  inter_C,                              // encoder type
                                  false,                                 // minimal
                                  pthash_search_type::mult_hash         // additive
                                                                        // displacement search
                                  >
        pthash_type;

    pthash_type f;

    /* Build the function in internal memory. */
    std::cout << "building the function..." << std::endl;
    auto start = clock_type::now();
    auto timings = f.build_in_internal_memory(keys.begin(), keys.size(), config);
    double total_microseconds = timings.partitioning_microseconds +
                                timings.mapping_ordering_microseconds +
                                timings.searching_microseconds + timings.encoding_microseconds;
    std::cout << "function built in " << to_microseconds(clock_type::now() - start) / 1000000
              << " seconds" << std::endl;
    std::cout << "computed: " << total_microseconds / 1000000 << " seconds" << std::endl;
    /* Compute and print the number of bits spent per key. */
    double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
    std::cout << "function uses " << bits_per_key << " [bits/key]" << std::endl;

    /* Sanity check! */
    if (check(keys.begin(), f)) std::cout << "EVERYTHING OK!" << std::endl;

    size_t queries=1e8;
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    std::vector<uint64_t > queryInputs;
    queryInputs.reserve(queries);
    for (uint64_t i = 0; i < queries; ++i) {
        uint64_t pos = dis(gen) % num_keys;
        queryInputs.push_back(keys[pos]);
    }

    essentials::timer<std::chrono::high_resolution_clock, std::chrono::nanoseconds> t;
    t.start();
    for (uint64_t i = 0; i < queries; ++i) {
        essentials::do_not_optimize_away(f(queryInputs[i]));
    }
    t.stop();
    double lookup_time = t.elapsed() / static_cast<double>(queries);
    std::cout << lookup_time << " [nanosec/key]" << std::endl;
    return 0;
}