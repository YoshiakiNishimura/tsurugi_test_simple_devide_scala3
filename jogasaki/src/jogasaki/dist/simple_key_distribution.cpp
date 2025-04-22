/*
 * Copyright 2018-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "simple_key_distribution.h"

#include <algorithm>
#include <iomanip>

namespace jogasaki::dist {

std::optional<double> simple_key_distribution::estimate_count(range_type const& /*range*/) {
    return std::nullopt;
}

std::optional<double> simple_key_distribution::estimate_key_size(range_type const& /*range*/) {
    return std::nullopt;
}

std::optional<double> simple_key_distribution::estimate_value_size(range_type const& /*range*/) {
    return std::nullopt;
}
std::vector<simple_key_distribution::pivot_type> simple_key_distribution::compute_pivots(
    size_type max_count, range_type const& range) {

    std::vector<pivot_type> pivots;

    if (max_count == 0) return pivots;

    constexpr int total_range = 512;  // 0x81 0x00 - 0x82 0xFF
    int step = total_range / (max_count + 1);

    for (std::size_t i = 1; i <= max_count; ++i) {
        int value = i * step; // absolute offset: 0 ~ 511
        uint8_t hi = static_cast<uint8_t>(0x81 + (value / 256));  // 0x81 or 0x82
        uint8_t lo = static_cast<uint8_t>(value % 256);

        std::string key = {static_cast<char>(hi), static_cast<char>(lo)};
#if 0
	std::cerr << "key =";
        for (unsigned char c : key) {
            std::cerr << " 0x" << std::hex << std::uppercase << std::setw(2)
                      << std::setfill('0') << static_cast<int>(c);
        }
        std::cerr << std::dec << std::endl;
#endif
        pivot_type pivot(key.data(), key.size());
        if ((range.begin_key().empty() || pivot >= range.begin_key()) &&
            (range.end_key().empty() || pivot < range.end_key())) {
            pivots.emplace_back(pivot);
        }
    }

    return pivots;
}

} // namespace jogasaki::dist
