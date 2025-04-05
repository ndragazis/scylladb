/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/net/inet_address.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "exceptions.hh"
#include "managed_identity_credentials.hh"

namespace azure {

static std::optional<sstring> filter_reply(const sstring& body) {
    static constexpr char REDACTED_VALUE[] = "[REDACTED]";
    if (!body.empty()) {
        auto j = rjson::parse(body);
        auto val = rjson::find(j, "access_token");
        if (val) {
            val->SetString(REDACTED_VALUE);
            return rjson::print(j);
        }
    }
    return std::nullopt;
}

managed_identity_credentials::managed_identity_credentials(const sstring& logctx)
    : credentials(logctx)
{}

access_token managed_identity_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "access_token");
    auto expires_in_str = rjson::get<std::string>(json, "expires_in");
    if (auto expires_in_int = std::atoi(expires_in_str.c_str())) {
        return { token, timeout_clock::now() + std::chrono::seconds(expires_in_int), resource_uri };
    }
    throw std::runtime_error(seastar::format("Invalid expires_in value: {}", expires_in_str));
}

// Token request from IMDS.
// https://docs.azure.cn/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    log_debug("Refreshing token");

    const auto op = httpd::operation_type::GET;
    const auto host = IMDS_HOST;
    const auto port = 80;
    const auto path = seastar::format(IMDS_TOKEN_PATH_TEMPLATE, IMDS_API_VERSION, resource_uri);

    auto print_request = [&] (const http::request& req) {
        auto linesep = "\n";
        fmt::memory_buffer buf;
        fmt::format_to(fmt::appender(buf), "{} http://{}{} HTTP/{}{}", type2str(op), host, path, req._version, linesep);
        for (auto& [k, v] : req._headers) {
            fmt::format_to(fmt::appender(buf), "{}: {}{}", k, v, linesep);
        }
        // No body.
        return to_string(buf);
    };
    auto print_reply = [&] (const http::reply& rep, const sstring& rep_body) {
        auto linesep = "\n";
        fmt::memory_buffer buf;
        auto s = rep.response_line();
        // remove the trailing \r\n from response_line string. we want our own linebreak, hence substr.
        fmt::format_to(fmt::appender(buf), "{}{}", std::string_view(s).substr(0, s.size()-2), linesep);
        for (auto& [k, v] : rep._headers) {
            fmt::format_to(fmt::appender(buf), "{}: {}{}", k, v, linesep);
        }
        fmt::format_to(fmt::appender(buf), "{}{}", linesep, filter_reply(rep_body).value_or(rep_body));
        return to_string(buf);
    };

    auto req = http::request::make(op, host, path);
    req._version = "1.1";

    log_trace("Sending request: {}", print_request(req));

    auto addr = seastar::net::inet_address(host);
    auto factory = std::make_unique<seastar::http::experimental::basic_connection_factory>(socket_address(addr, port));
    http::experimental::client http_client{std::move(factory)};

    sstring resp;
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
        auto lin = std::move(in);
        resp = co_await util::read_entire_stream_contiguous(lin);
        if (rep._status == http::reply::status_type::ok) {
            log_trace("Got response: {}", print_reply(rep, resp));
        } else {
            log_trace("Got unexpected response: {}", print_reply(rep, resp));
            throw creds_auth_error::make_error(rep._status, resp);
        }
    }).finally([&] -> future<> { co_await http_client.close(); });
    token = make_token(rjson::parse(resp), resource_uri);
}

}