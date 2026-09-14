/** @file common.hpp
 *  @brief Small CLI and sample reporting helpers; timings go to stdout only. */
#pragma once
#include <algorithm>
#include <charconv>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace bench {
using clock = std::chrono::steady_clock;
/** @brief Parse a bounded positive count. @param text Decimal input. @param max Limit. */
inline unsigned count(std::string_view text, unsigned max = 100'000'000) {
    unsigned value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || !value || value > max)
        throw std::invalid_argument("count is zero, invalid, or too large");
    return value;
}
/** @brief Convert elapsed monotonic time to nanoseconds. @param start Start time. */
inline double elapsed(clock::time_point start) {
    return std::chrono::duration<double, std::nano>(clock::now() - start).count();
}
/** @brief Print distribution of run means, not individual operation latency.
 *  @param name Workload label. @param samples Nanoseconds per operation for each run. */
inline void report(std::string_view name, std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    std::cout << name << " ns/op: median=" << samples[samples.size() / 2]
              << " min=" << samples.front() << " max=" << samples.back() << '\n';
}
} // namespace bench
