#pragma once

#include <chrono>

namespace jogasaki::metrics {

struct decode_timings {
    using duration_type              = std::chrono::nanoseconds;
    duration_type start_ms           = duration_type::zero();
    duration_type end_ms             = duration_type::zero();
    duration_type duration_ms        = duration_type::zero();
    duration_type loop_time          = duration_type::zero();
    duration_type cancel_check       = duration_type::zero();
    duration_type kvs_next           = duration_type::zero();
    duration_type read_key_ms        = duration_type::zero();
    duration_type read_value         = duration_type::zero();
    duration_type field_process      = duration_type::zero();
    duration_type downstream_process = duration_type::zero();
    duration_type yield_check        = duration_type::zero();
    void dump(std::ostream& out) const noexcept { out << "decode_timings:\n"; }
    void dump_csv(std::ostream& out) const noexcept {
        auto measured_total = cancel_check + kvs_next + read_key_ms + read_value + field_process +
                              downstream_process + yield_check;
        auto unmeasured_time = loop_time.count() - measured_total.count();
        out << start_ms.count() << "," << end_ms.count() << "," << duration_ms.count() << ","
            << loop_time.count() << "," << unmeasured_time << "," << cancel_check.count()
            << "," << kvs_next.count() << "," << read_key_ms.count() << "," << read_value.count()
            << "," << field_process.count() << "," << downstream_process.count() << ","
            << yield_check.count() << std::endl;
    }
};

} // namespace jogasaki::metrics
