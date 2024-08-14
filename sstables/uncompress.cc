/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <cstdint>
#include <seastar/core/fstream.hh>

#include "seastar/core/future.hh"
#include "types.hh"
#include "seastar/core/temporary_buffer.hh"

namespace sstables {

class uncompressed_file_data_source_impl : public data_source_impl {
    std::optional<input_stream<char>> _input_stream;
    checksum& _checksum;
    uint64_t _underlying_pos;
    uint64_t _pos;
    //FIXME: Do we actually need this data member?
    uint64_t _beg_pos;
    uint64_t _end_pos;
public:
    uncompressed_file_data_source_impl(file f, checksum& checksum,
    uint64_t pos, size_t len, file_input_stream_options options)
    : _input_stream(make_file_input_stream(std::move(f), pos, len, std::move(options)))
    , _checksum(checksum)
    , _underlying_pos(pos)
    , _pos(pos)
    , _beg_pos(pos)
    , _end_pos(pos + len)
    {}

    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        //FIXME:
        //What if the _pos is not aligned at the chunk size?
        return _input_stream->read_exactly(_checksum.chunk_size).then([this](temporary_buffer<char> buf) {
            _pos += buf.size();
            return buf;
        });
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        _pos += n;
        SCYLLA_ASSERT(_pos <= _end_pos);
        if (_pos == _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        _beg_pos = _pos;
        return _input_stream->skip(n).then([] {
            return make_ready_future<temporary_buffer<char>>();
        });
    }
};

class uncompressed_file_data_source : public data_source {
public:
    uncompressed_file_data_source(file f, checksum& checksum, uint64_t offset,
    size_t len, file_input_stream_options options)
    : data_source(std::make_unique<uncompressed_file_data_source_impl>(
        std::move(f), checksum, offset, len, std::move(options)))
    {}
};

inline input_stream<char> make_uncompressed_file_input_stream(
    file f, checksum& checksum, uint64_t offset, size_t len,
    file_input_stream_options options)
{
    return input_stream<char>(uncompressed_file_data_source(
        std::move(f), checksum, offset, len, std::move(options)));
}

}