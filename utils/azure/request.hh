/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/algorithm/string.hpp>

#include <seastar/http/common.hh>

#include "utils/log.hh"

using namespace seastar;

namespace azure {

class http_log_filter {
public:
    static constexpr char REDACTED_VALUE[] = "[REDACTED]";

    enum class body_type {
        request,
        response,
    };

    using sstring_opt = std::optional<sstring>;
    // Filter a request/response header.
    // Returns an optional containing the filtered value. If no filtering is required, the optional is not engaged.
    virtual sstring_opt filter_header(const sstring& name, const sstring& value) const { return std::nullopt; }
    // Filter the request/response body.
    // Returns an optional containing the filtered value. If no filtering is required, the optional is not engaged.
    virtual sstring_opt filter_body(body_type type, const sstring& body) const { return std::nullopt; }
};

class authz_log_filter : public http_log_filter {
public:
    virtual sstring_opt filter_header(const sstring& name, const sstring& value) const override {
        if (boost::iequals(name, "Authorization") && value.starts_with("Bearer")) {
            return REDACTED_VALUE;
        }
        return std::nullopt;
    }
};

extern http_log_filter noop_filter;

// A utility function to send HTTP(S) requests.
//
// Features:
// - Emits trace logs for both the request and the response.
// - Supports an optional log filter to redact sensitive information.
// - Does not wait on TLS termination.
using key_values = std::initializer_list<std::pair<std::string_view, std::string_view>>;
future<sstring> send_request(const sstring& host, int port, const sstring& path,
        bool use_https, const sstring& body, const sstring& mime_type,
        httpd::operation_type op, key_values headers,
        logging::logger& logger, const http_log_filter& filter = noop_filter,
        const sstring& truststore = "", const sstring& priority_string = "");

}