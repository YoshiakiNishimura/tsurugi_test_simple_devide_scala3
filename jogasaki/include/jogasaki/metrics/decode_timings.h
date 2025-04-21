#pragma once

#include <chrono>

namespace jogasaki::metrics {

struct decode_timings {
    using duration_type                   = std::chrono::nanoseconds;
    duration_type index_field_mapper_process = duration_type::zero();
    duration_type populate_field_variables = duration_type::zero();
    duration_type decode_fields = duration_type::zero();
    duration_type total                   = duration_type::zero();
    duration_type consume_stream          = duration_type::zero();
    duration_type consume_stream_nullable = duration_type::zero();
    duration_type decode                  = duration_type::zero();
    duration_type decode_nullable         = duration_type::zero();
    duration_type set_null                = duration_type::zero();
    void dump(std::ostream& out) const noexcept {
        out << "decode_timings:\n"
            << "  total:          " << total.count() << " ns\n"
            << "  consume_stream:          " << consume_stream.count() << " ns\n"
            << "  consume_stream_nullable: " << consume_stream_nullable.count() << " ns\n"
            << "  decode:                 " << decode.count() << " ns\n"
            << "  decode_nullable:        " << decode_nullable.count() << " ns\n"
            << "  set_null:               " << set_null.count() << " ns\n";
    }
    void dump_csv(std::ostream& out) const noexcept {
        out << total.count() << "," << index_field_mapper_process.count() << "," <<
        populate_field_variables.count() <<"," << decode_fields.count()<<"," 
        << consume_stream.count() << ","
            << consume_stream_nullable.count() << "," << decode.count() << ","
            << decode_nullable.count() << "," << set_null.count() << "\n";
    }
};

} // namespace jogasaki::metrics
