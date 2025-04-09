/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/algorithm/string/trim.hpp>

#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>

#include "request.hh"
#include "db/config.hh"
#include "utils/http.hh"

namespace azure {

auto noop_filter = http_log_filter();

static future<::shared_ptr<tls::certificate_credentials>> make_creds(const sstring& truststore, const sstring& priority_string) {
    auto creds = seastar::make_shared<tls::certificate_credentials>();
    if (!priority_string.empty()) {
        creds->set_priority_string(priority_string);
    } else {
        creds->set_priority_string(db::config::default_tls_priority);
    }
    if (!truststore.empty()) {
        co_await creds->set_x509_trust_file(truststore, seastar::tls::x509_crt_format::PEM);
    } else {
        co_await creds->set_system_trust();
    }
    co_return creds;
}

future<sstring> send_request(const sstring& host, int port, const sstring& path,
        bool use_https, const sstring& body, const sstring& mime_type,
        httpd::operation_type op, key_values headers,
        logging::logger& logger, const http_log_filter& filter,
        const sstring& truststore, const sstring& priority_string) {
    auto req = http::request::make(op, host, path);
    req._version = "1.1"; // must be set before calling request_line()
    for (auto& [k, v] : headers) {
        req._headers[sstring(k)] = sstring(v);
    }
    if (!body.empty()) {
        req.write_body("", std::move(body));
        req.set_mime_type(mime_type);
    }

    if (logger.is_enabled(log_level::trace)) {
        logger.trace("Sending HTTP request:");
        logger.trace("http{}://{}:{} \"{}\"", use_https ? "s" : "", host, port, boost::trim_copy(req.request_line()));
        for (auto& [k, v] : req._headers) {
            logger.trace("{}: {}", k, filter.filter_header(k, v).value_or(v));
        }
        logger.trace("{}", filter.filter_body(http_log_filter::body_type::request, body).value_or(body));
    }

    // Azure Key Vault and Azure Entra do not respond to TLS close_notify alert
    // as they should per the standard: https://www.rfc-editor.org/rfc/rfc5246#section-7.2.1.
    // This causes a 10-second stall on TLS termination, because Seastar waits
    // for that long before closing the socket (refer to `seastar::tls::session::close()`).
    // Set `tls_wait_on_close::no` to close the socket immediately.
    auto factory = std::make_unique<utils::http::dns_connection_factory>(
            host,
            port,
            use_https,
            logger,
            use_https ? co_await make_creds(truststore, priority_string) : nullptr,
            utils::http::dns_connection_factory::tls_wait_on_close::no);

    http::experimental::client http_client{std::move(factory)};
    sstring resp;
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            resp = co_await util::read_entire_stream_contiguous(lin);
            if (rep._status == http::reply::status_type::ok) {
                if (logger.is_enabled(log_level::trace)) {
                    logger.trace("Got response {}: {}", int(rep._status), filter.filter_body(http_log_filter::body_type::response, resp).value_or(resp));
                }
            } else {
                if (logger.is_enabled(log_level::trace)) {
                    logger.trace("Got unexpected response ({})", rep._status);
                    for (auto& [k, v] : rep._headers) {
                        logger.trace("{}: {}", k, filter.filter_header(k, v).value_or(v));
                    }
                    logger.trace("{}", filter.filter_body(http_log_filter::body_type::response, resp).value_or(resp));
                }
                throw seastar::httpd::unexpected_status_error(rep._status);
            }
        }).finally([&] -> future<> { co_await http_client.close(); });
    co_return resp;
}

}
