#include <iostream>
#include <cmath>
#include <limits>

#include "utils/bucketers.hpp"

std::random_device rd;

// Single run of the experiment for a given bucketer; returns total trials.
template <typename Bucketer>
double run_once(std::size_t n, std::size_t buckets) {
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);

    Bucketer b;
    b.init(buckets, double(n) / double(buckets), n, 1.0);

    std::vector<std::size_t> sizes(buckets);

    for (std::size_t i = 0; i < n; ++i) {
        sizes[b.bucket(dist(gen))]++;
    }

    std::sort(sizes.begin(), sizes.end(), std::greater<std::size_t>());

    std::size_t inserted = 0;
    std::size_t trials = 0;
    std::size_t i = 0;
    while (i < buckets) {
        std::size_t s = sizes[i];
        for (std::size_t j = 0; j < s; ++j) {
            ++trials;
            uint64_t rnd = dist(gen);
            uint64_t u = (static_cast<__uint128_t>(rnd) * static_cast<uint64_t>(n)) >> 64;

            if (u < static_cast<uint64_t>(inserted + j)) {
                --i;
                inserted -= s;
                break;
            }
        }
        inserted += s;
        ++i;
    }

    return trials;
}

// Average trials per key over multiple repetitions.
template <typename Bucketer>
double average_trials_per_key(std::size_t n, std::size_t buckets, std::size_t repetitions) {
    double total_trials = 0.0;
    for (std::size_t r = 0; r < repetitions; ++r) {
        total_trials += static_cast<double>(run_once<Bucketer>(n, buckets));
    }
    return total_trials / (static_cast<double>(repetitions) * static_cast<double>(n));
}

// Lower bound B * (n^n / n!)^(1/B), returned as total trials (not per key).
double lower_bound_value(std::size_t n, std::size_t B) {
    if (n == 0 || B == 0) {
        return 0.0;
    }

    double nd = static_cast<double>(n);
    double Bd = static_cast<double>(B);

    // Compute log(n^n / n!) = n * log(n) - log(n!) using lgamma for numerical stability.
    double log_n_factorial = std::lgamma(nd + 1.0);
    double log_term = nd * std::log(nd) - log_n_factorial;

    // Exponent for (n^n / n!)^(1/B).
    double exponent = log_term / Bd;

    // Guard against overflow in exp for extremely large inputs.
    if (exponent > 700.0) { // ~log(DBL_MAX)
        return std::numeric_limits<double>::infinity();
    }

    double inner = std::exp(exponent);
    return Bd * inner;
}

double alternative_lower_bound_value(std::size_t n, std::size_t B) {
    if (n == 0 || B == 0) {
        return 0.0;
    }

    double nd = static_cast<double>(n);
    double Bd = static_cast<double>(B);

    // Compute log of the alternative lower bound
    //   B * (e^(n/B) - 1) * pi^2 / 6
    // in a numerically stable way when possible.

    double ratio = nd / Bd; // n / B

    // log(e^(ratio) - 1) using expm1 when safe and accurate.
    double log_exp_minus_one;
    if (ratio < 700.0) { // avoid overflow in expm1/exp
        double exp_minus_one = std::expm1(ratio); // e^(ratio) - 1 with good accuracy for small ratio
        if (exp_minus_one <= 0.0) {
            // Underflow or extremely small; treat as 0 lower bound
            return 0.0;
        }
        log_exp_minus_one = std::log(exp_minus_one);
    } else {
        // For large ratio, e^(ratio) - 1 ~ e^(ratio), so log(e^(ratio) - 1) ~ ratio.
        log_exp_minus_one = ratio;
    }

    // pi^2 / 6 factor
    const double pi = std::acos(-1.0);
    const double log_pi_sq_over_6 = 2.0 * std::log(pi) - std::log(6.0);

    double log_B = std::log(Bd);
    double log_alt = log_B + log_exp_minus_one + log_pi_sq_over_6;

    // Guard against overflow when exponentiating.
    if (log_alt > 700.0) { // ~log(DBL_MAX)
        return std::numeric_limits<double>::infinity();
    }

    return std::exp(log_alt);
}

int main() {
    // Two plots with different n values.
    const std::vector<std::size_t> ns = {2500};

    // X-axis: average bucket size = n / B.
    const std::vector<double> avg_bucket_sizes = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};

    const std::size_t repetitions = 20;  // repetitions per configuration

    for (std::size_t n : ns) {
        for (double avg_bucket_size : avg_bucket_sizes) {
            std::size_t buckets = static_cast<std::size_t>(std::llround(static_cast<double>(n) / avg_bucket_size));
            if (buckets == 0) continue;

            double lambda = static_cast<double>(n) / static_cast<double>(buckets);

            // Lower bound series (analytic), in trials per key.
            double lb_total = lower_bound_value(n, buckets);
            double lb_per_key = lb_total / static_cast<double>(n);

            double lb2_total = alternative_lower_bound_value(n, buckets);
            double lb2_per_key = lb2_total / static_cast<double>(n);

            // Empirical series: opt bucketer and skew bucketer.
            //double opt_trials_per_key = average_trials_per_key<pthash::opt_bucketer<>>(n, buckets, repetitions);
            double beta_trials_per_key = average_trials_per_key<pthash::opt_bucketer<false>>(n, buckets, repetitions);
            //double skew_trials_per_key = average_trials_per_key<pthash::skew_bucketer>(n, buckets, repetitions);
            //double unif_trials_per_key = average_trials_per_key<pthash::uniform_bucketer>(n, buckets, repetitions);

            // Lower bound line
            /*std::cout<<"RESULT n="<<n;
            std::cout<<" avg_bucket_size="<<lambda;
            std::cout<<" bucketer_type=lower_bound";
            std::cout<<" avg_trials_per_key="<<lb_per_key;
            std::cout<<std::endl;

             // Lower bound line
            std::cout<<"RESULT n="<<n;
            std::cout<<" avg_bucket_size="<<lambda;
            std::cout<<" bucketer_type=lower_bound_alt";
            std::cout<<" avg_trials_per_key="<<lb2_per_key;
            std::cout<<std::endl;

            // Opt bucketer line
            std::cout<<"RESULT n="<<n;
            std::cout<<" avg_bucket_size="<<lambda;
            std::cout<<" bucketer_type=opt";
            std::cout<<" avg_trials_per_key="<<opt_trials_per_key;
            std::cout<<std::endl;*/

            // Beta bucketer line
            std::cout<<"RESULT n="<<n;
            std::cout<<" avg_bucket_size="<<lambda;
            std::cout<<" bucketer_type=beta";
            std::cout<<" avg_trials_per_key="<<beta_trials_per_key;
            std::cout<<std::endl;


            // Skewed bucketer line
            /*std::cout<<"RESULT n="<<n;
            std::cout<<" avg_bucket_size="<<lambda;
            std::cout<<" bucketer_type=skewed";
            std::cout<<" avg_trials_per_key="<<skew_trials_per_key;
            std::cout<<std::endl;*/

            // Uniform bucketer line
            /*std::cout<<"RESULT n="<<n;
            std::cout<<" avg_bucket_size="<<lambda;
            std::cout<<" bucketer_type=uniform";
            std::cout<<" avg_trials_per_key="<<unif_trials_per_key;
            std::cout<<std::endl;*/
        }
    }

    return 0;
}