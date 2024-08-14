/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <cstdint>
#include <seastar/core/fstream.hh>

#include "seastar/core/future.hh"
#include "seastar/core/temporary_buffer.hh"

#include "types.hh"
#include "exceptions.hh"

namespace sstables {

// File data source implementation for uncompressed SSTables
class uncompressed_file_data_source_impl : public data_source_impl {
    std::optional<input_stream<char>> _input_stream;
    checksum& _checksum;
    uint64_t _underlying_pos;
    uint64_t _pos;
    uint64_t _beg_pos;
    uint64_t _end_pos;
public:
    uncompressed_file_data_source_impl(file f, checksum& checksum,
    uint64_t pos, size_t len, file_input_stream_options options)
    : _checksum(checksum)
    , _pos(pos)
    , _beg_pos(pos)
    , _end_pos(pos + len)
    {
        // _beg_pos and _end_pos specify positions in the stream.
        // These are not necessarily aligned on chunk boundaries.
        // To be able to verify the checksums, we need to translate
        // them into a range of chunks that contain the given
        // address range, and open a file input stream to read that
        // range. The _underlying_pos always points to the current
        // chunk-aligned position of the file input stream.
        auto chunk_size = checksum.chunk_size;
        auto start = (_beg_pos / chunk_size) * chunk_size;
        auto end = (_end_pos / chunk_size + 1) * chunk_size;
        _input_stream = make_file_input_stream(std::move(f), start, end - start, std::move(options));
        _underlying_pos = start;
    }

    virtual future<temporary_buffer<char>> get() override {
        auto chunk_size = _checksum.chunk_size;
        if (_pos >= _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        //FIXME:
        //Do I need the check for the out-of-sync reader?
        return _input_stream->read_exactly(chunk_size).then([this, chunk_size](temporary_buffer<char> buf) {
            if (buf.size() != chunk_size) {
                throw sstables::malformed_sstable_exception(format("uncompressed reader hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}", _underlying_pos, chunk_size, buf.size()));
            }
            buf.trim_front(_pos % chunk_size);
            _pos += buf.size();
            _underlying_pos += chunk_size;
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
        auto chunk_size = _checksum.chunk_size;
        _pos += n;
        SCYLLA_ASSERT(_pos <= _end_pos);
        if (_pos == _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        auto underlying_n = (_pos / chunk_size) * chunk_size - _underlying_pos;
        _beg_pos = _pos;
        return _input_stream->skip(underlying_n).then([] {
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