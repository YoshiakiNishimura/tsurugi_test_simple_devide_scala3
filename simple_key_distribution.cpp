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
    static const unsigned char prefix = 0x81;

    std::vector<uint8_t> offsets;
    uint8_t step = 0;
    uint8_t start = 0;

    switch (max_count) {
        case 1:  start = 0x80; step = 0x80; break;
        case 3:  start = 0x40; step = 0x40; break;
        case 7:  start = 0x20; step = 0x20; break;
        case 15: start = 0x10; step = 0x10; break;
        case 31: start = 0x08; step = 0x08; break;
        case 63: start = 0x04; step = 0x04; break;
        case 127: start = 0x02; step = 0x02; break;
        case 254: start = 0x01; step = 0x01; break;
        default: return {};
    }
    for (std::size_t i = 0; i < max_count; ++i) {
        uint8_t value = start + i * step;
        offsets.push_back(value);
#if 0
        std::cout << "offset: 0x"
                  << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(value) << std::endl;
#endif
    }

    pivots.reserve(offsets.size());

    for (auto offset : offsets) {
        std::string key = {static_cast<char>(prefix), static_cast<char>(offset)};
        pivot_type pivot(key.data(), key.size());
        if ((range.begin_key().empty() || pivot >= range.begin_key()) &&
            (range.end_key().empty() || pivot < range.end_key())) {
            pivots.emplace_back(pivot);
        }
    }
#if 0
    std::cerr << "pivots " << pivots.size() << std::endl;
#endif
    return pivots;
}

} // namespace jogasaki::dist
