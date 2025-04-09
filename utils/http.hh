/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/http/client.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/tls.hh>

#include "seastarx.hh"
#include "utils/log.hh"

namespace utils::http {

class dns_connection_factory : public seastar::http::experimental::connection_factory {
protected:
    std::string _host;
    int _port;
    logging::logger& _logger;
    bool _tls_wait_on_close;
    struct state {
        bool initialized = false;
        socket_address addr;
        ::shared_ptr<tls::certificate_credentials> creds;
        state() = default;
        state(::shared_ptr<tls::certificate_credentials> creds) : creds(std::move(creds)) {}
    };
    lw_shared_ptr<state> _state;
    shared_future<> _done;

    // This method can out-live the factory instance, in case `make()` is never called before the instance is destroyed.
    static future<> initialize(lw_shared_ptr<state> state, std::string host, int port, bool use_https, logging::logger& logger) {
        co_await coroutine::all(
            [state, host, port] () -> future<> {
                auto hent = co_await net::dns::get_host_by_name(host, net::inet_address::family::INET);
                state->addr = socket_address(hent.addr_list.front(), port);
            },
            [state, use_https] () -> future<> {
                if (use_https && !state->creds) {
                    tls::credentials_builder cbuild;
                    co_await cbuild.set_system_trust();
                    state->creds = cbuild.build_certificate_credentials();
                }
            }
        );

        state->initialized = true;
        logger.debug("Initialized factory, address={} tls={}", state->addr, state->creds == nullptr ? "no" : "yes");
    }

public:
    using tls_wait_on_close = bool_class<class tls_wait_on_close_tag>;
    dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, ::shared_ptr<tls::certificate_credentials> creds = nullptr, tls_wait_on_close wait = tls_wait_on_close::yes)
        : _host(std::move(host))
        , _port(port)
        , _logger(logger)
        , _tls_wait_on_close(wait)
        , _state(make_lw_shared<state>(std::move(creds)))
        , _done(initialize(_state, _host, _port, use_https, _logger))
    {
    }

    virtual future<connected_socket> make(abort_source*) override {
        if (!_state->initialized) {
            _logger.debug("Waiting for factory to initialize");
            co_await _done.get_future();
        }

        if (_state->creds) {
            _logger.debug("Making new HTTPS connection addr={} host={}", _state->addr, _host);
            co_return co_await tls::connect(_state->creds, _state->addr, tls::tls_options{ .wait_for_eof_on_shutdown = _tls_wait_on_close, .server_name = _host});
        } else {
            _logger.debug("Making new HTTP connection");
            co_return co_await seastar::connect(_state->addr, {}, transport::TCP);
        }
    }
};

} // namespace utils::http
