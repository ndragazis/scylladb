/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/http/common.hh>

#include "utils/log.hh"

using namespace seastar;

namespace azure {

// A utility function to send HTTP(S) requests.
//
// Features:
// - Emits trace logs for both the request and the response.
// - Supports an optional log filter to redact sensitive information.
// - Does not wait on TLS termination.
using key_values = std::initializer_list<std::pair<std::string_view, std::string_view>>;
future<sstring> send_request(const sstring& host, int port, const sstring& path,
        bool use_https, const sstring& body, const sstring& mime_type,
        httpd::operation_type op, key_values headers, logging::logger& logger,
        const sstring& truststore = "", const sstring& priority_string = "");

}