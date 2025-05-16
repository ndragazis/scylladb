/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/regex.hpp>

#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/core/sleep.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "request.hh"
#include "exceptions.hh"
#include "managed_identity_credentials.hh"

namespace azure {

static constexpr char REDACTED_VALUE[] = "[REDACTED]";

static body_filter make_response_filter() {
    return [](std::string_view body) -> std::optional<std::string> {
        if (!body.empty()) {
            auto j = rjson::parse(body);
            auto val = rjson::find(j, "access_token");
            if (val) {
                val->SetString(REDACTED_VALUE);
                return rjson::print(j);
            }
        }
        return std::nullopt;
    };
}

managed_identity_credentials::managed_identity_credentials(const sstring& endpoint, const sstring& logctx)
    : credentials(logctx)
    , _host(IMDS_HOST)
    , _port(IMDS_PORT)
{
    if (endpoint.empty()) {
        return;
    }
    static const boost::regex uri_pattern(R"((?:(https?):\/\/)?([^/:]+)(?::(\d+))?)");
    boost::smatch match;
    std::string tmp{endpoint};
    if (boost::regex_match(tmp, match, uri_pattern)) {
        std::string scheme = match[1];
        std::string host = match[2];
        std::string port_str = match[3];
        if (!scheme.empty() && scheme != "http") {
            throw std::invalid_argument(fmt::format("Unsupported scheme: {}", scheme));
        }
        _host = host;
        if (!port_str.empty()) {
            _port = std::stoi(port_str);
        }
    } else {
        throw std::invalid_argument(fmt::format("Invalid endpoint format: {}", endpoint));
    }
}

access_token managed_identity_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "access_token");
    auto expires_in_str = rjson::get<std::string>(json, "expires_in");
    if (auto expires_in_int = std::atoi(expires_in_str.c_str())) {
        return { token, timeout_clock::now() + std::chrono::seconds(expires_in_int), resource_uri };
    }
    throw std::runtime_error(seastar::format("Invalid expires_in value: {}", expires_in_str));
}

/**
 * @brief Retries for transient errors.
 *
 * Retries are performed for 404, 429, and 5xx errors, using an exponential backoff strategy.
 * The first retry is immediate, while the rest follow an exponential delay, starting at 2 seconds.
 *
 * Based on the official Azure docs:
 * https://docs.azure.cn/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#retry-guidance
 *
 * @note The number of retries was set to 3 instead of the recommended 5.
 * Waiting for 2 + 6 seconds seems too much already and 3 is the default value in the Azure C++ SDK:
 * https://github.com/Azure/azure-sdk-for-cpp/blob/126452efd30860263398a152f11f337007f529f4/sdk/core/azure-core/inc/azure/core/http/policies/policy.hpp#L113
 *
 * @param func A callable that returns a future<>, representing the asynchronous operation to retry.
 * @return A future<> that resolves to the result of the operation if successful, or propagates the error if all retries fail.
 * @throws creds_auth_error If the operation fails after the maximum number of retries or encounters a non-retriable error.
 */
future<> managed_identity_credentials::with_retries(std::function<future<>()> func) {
    constexpr int MAX_RETRIES = 3;
    constexpr std::chrono::milliseconds DELTA_BACKOFF {2000};
    std::chrono::milliseconds backoff;

    int retries = 0;
    while (true) {
        try {
            co_return co_await func();
        } catch (const creds_auth_error& e) {
            auto status = e.status();
            bool should_retry =
                    status == http::reply::status_type::not_found ||
                    status == http::reply::status_type::too_many_requests ||
                    http::reply::classify_status(status) == http::reply::status_class::server_error;

            if (retries >= MAX_RETRIES || !should_retry) {
                throw;
            }

            backoff = DELTA_BACKOFF * ((1 << retries) - 1);
            log_info("Token request failed with status {}. Reason: {}. Retrying in {} ms...",
                    static_cast<int>(status), e.what(), backoff.count());

            retries++;
        }
        co_await seastar::sleep(backoff);
    }
}

// Token request from IMDS.
// https://docs.azure.cn/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    log_debug("Refreshing token");

    co_await with_retries([this, &resource_uri] () -> future<> {

        const auto op = httpd::operation_type::GET;
        const auto path = seastar::format(IMDS_TOKEN_PATH_TEMPLATE, IMDS_API_VERSION, resource_uri);

        auto req = http::request::make(op, _host, path);
        req._version = "1.1";
        req._headers["Metadata"] = "true";

        if (az_creds_logger.is_enabled(log_level::trace)) {
            log_trace("Sending request: {}", format_request(req));
        }

        auto addr = co_await net::dns::resolve_name(_host, net::inet_address::family::INET);
        auto factory = std::make_unique<seastar::http::experimental::basic_connection_factory>(socket_address(addr, uint64_t(_port)));
        http::experimental::client http_client{std::move(factory)};

        sstring resp;
        co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            resp = co_await util::read_entire_stream_contiguous(lin);
            if (rep._status == http::reply::status_type::ok) {
                if (az_creds_logger.is_enabled(log_level::trace)) {
                    log_trace("Got response: {}", format_reply(rep, resp, make_response_filter()));
                }
            } else {
                if (az_creds_logger.is_enabled(log_level::trace)) {
                    log_trace("Got unexpected response: {}", format_reply(rep, resp));
                }
                throw creds_auth_error::make_error(rep._status, resp);
            }
            token = make_token(rjson::parse(resp), resource_uri);
        }).finally([&] -> future<> { co_await http_client.close(); });
    });
}

}