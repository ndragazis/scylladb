/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/iostream.hh>

#include "sstables/types.hh"

namespace sstables {

using integrity_error_handler_t = std::function<void(seastar::sstring)>;

input_stream<char> make_checksummed_file_k_l_format_input_stream(file f,
                uint64_t file_len, const sstables::checksum& checksum,
                uint64_t offset, size_t len,
                class file_input_stream_options options,
                std::optional<uint32_t> digest,
                integrity_error_handler_t error_handler);

input_stream<char> make_checksummed_file_m_format_input_stream(file f,
                uint64_t file_len, const sstables::checksum& checksum,
                uint64_t offset, size_t len,
                class file_input_stream_options options,
                std::optional<uint32_t> digest,
                integrity_error_handler_t error_handler);
}