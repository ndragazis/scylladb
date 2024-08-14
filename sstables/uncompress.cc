/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <cstdint>
#include <seastar/core/fstream.hh>

#include "reader_permit.hh"
#include "seastar/core/temporary_buffer.hh"

class uncompressed_file_data_source_impl : public data_source_impl {
    std::optional<input_stream<char>> _input_stream;
    reader_permit _permit;
    uint64_t _underlying_pos;
    uint64_t _pos;
    uint64_t _beg_pos;
    uint64_t _end_pos;
public:
    uncompressed_file_data_source_impl(file f, uint64_t pos, size_t len,
    file_input_stream_options options, reader_permit permit)
    : _input_stream(make_file_input_stream(std::move(f), pos, len, std::move(options)))
    , _permit(std::move(permit))
    , _underlying_pos(pos)
    , _pos(pos)
    , _beg_pos(pos)
    , _end_pos(pos + len)
    {}

    virtual future<temporary_buffer<char>> get() override {
        return _input_stream->read_exactly(42).then([this, addr](temporary_buffer<char> buf) {
            _pos += buf.size();
            return make_tracked_temporary_buffer(std::move(buf), std::move(res_units));
        });
    }
};

class uncompressed_file_data_source : public data_source {
public:
    uncompressed_file_data_source(file f, uint64_t offset, size_t len,
    file_input_stream_options options, reader_permit permit)
    : data_source(std::make_unique<uncompressed_file_data_source_impl>(
        std::move(f), offset, len, std::move(options), std::move(permit)))
    {}
};

inline input_stream<char> make_uncompressed_file_input_stream(
    file f, uint64_t offset, size_t len, file_input_stream_options options,
    reader_permit permit)
{
    return input_stream<char>(uncompressed_file_data_source(
        std::move(f), offset, len, std::move(options), std::move(permit)));
}