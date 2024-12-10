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
#if 1
    if (max_count == 1) {
        std::string key = {static_cast<char>(prefix), static_cast<char>(0x80)};
        pivot_type pivot(key.data(), key.size());
        if ((range.begin_key().empty() || pivot >= range.begin_key()) &&
            (range.end_key().empty() || pivot < range.end_key())) {
        pivots.emplace_back(pivot);
            }
    }
    if (max_count == 3) {
        std::vector<uint8_t> offsets = {0x40, 0x80,0xc0};
        for (auto offset : offsets) {
            std::string key = {static_cast<char>(prefix), static_cast<char>(offset)};
            pivot_type pivot(key.data(), key.size());
            pivots.emplace_back(pivot);
        }
    }

    if (max_count == 7) {
        std::vector<uint8_t> offsets = {0x20, 0x40, 0x60, 0x80, 0xa0, 0xc0, 0xe0};

        for (auto offset : offsets) {
            std::string key = {static_cast<char>(prefix), static_cast<char>(offset)};
            pivot_type pivot(key.data(), key.size());
            pivots.emplace_back(pivot);
        }
    }
    if (max_count == 15) {
        std::vector<uint8_t> offsets = {
            0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0,0xf0};

        for (auto offset : offsets) {
            std::string key = {static_cast<char>(prefix), static_cast<char>(offset)};
            pivot_type pivot(key.data(), key.size());
            pivots.emplace_back(pivot);
        }
    }
#else
    for (int i = 0; i <= 0xff; ++i) {
        std::string key = {static_cast<char>(prefix), static_cast<char>(i)};
        pivot_type pivot(key.data(), key.size());

        if ((range.begin_key().empty() || pivot >= range.begin_key()) &&
            (range.end_key().empty() || pivot < range.end_key())) {
            pivots.emplace_back(pivot);
        }
    }

    if (max_count < pivots.size()) {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(pivots.begin(), pivots.end(), g);
        pivots.resize(max_count);
    }

    std::sort(pivots.begin(), pivots.end());
#endif
    return pivots;
}

} // namespace jogasaki::dist