/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "utils/http.hh"
#include "azure_credentials.hh"

static logger azcredlog("azure_credentials");

namespace azure {

access_token::access_token(const rjson::value& json, const resource_type& resource_uri)
    : token(rjson::get<std::string>(json, "access_token"))
    , expiry(timeout_clock::now() + std::chrono::seconds(rjson::get<int>(json, "expires_in")))
    , resource_uri(resource_uri)
{}

bool access_token::empty() const {
    return token.empty();
}

bool access_token::expired() const {
    if (empty()) {
        return true;
    }
    return timeout_clock::now() >= this->expiry;
}

future<access_token> credentials::get_access_token(const resource_type& resource_uri) {
    if (token.expired() || token.resource_uri != resource_uri) {
        co_await refresh(resource_uri);
    }
    co_return token;
}

service_principal_credentials::service_principal_credentials(const sstring& tenant_id, const sstring& client_id, const sstring& client_secret, const sstring& client_cert)
        : _tenant_id(tenant_id)
        , _client_id(client_id)
        , _client_secret(client_secret)
        , _client_cert(client_cert)
{}

sstring service_principal_credentials::get_token_host() {
    return AZURE_ENTRA_ID_HOST;
}

sstring service_principal_credentials::get_token_path() {
    return seastar::format("/{}/oauth2/v2.0/token", _tenant_id);
}

future<> service_principal_credentials::refresh(const resource_type& resource_uri) {
    azcredlog.debug("Refreshing token for principal: {}", _client_id);
    if (_client_secret != "") {
        co_await refresh_with_secret(resource_uri);
    } else {
        co_await refresh_with_certificate(resource_uri);
    }
}

// Token request with secret.
// Based on: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret
future<> service_principal_credentials::refresh_with_secret(const resource_type& resource_uri) {
    // Scopes for the client credentials flow must contain only one resource
    // identifier and only the .default scope.
    auto scope = seastar::format("{}/.default", resource_uri);
    sstring grant_type = "client_credentials";
    sstring body = seastar::format(
            "client_id={}&scope={}&client_secret={}&grant_type={}",
            _client_id,
            seastar::http::internal::url_encode(scope),
            seastar::http::internal::url_encode(_client_secret),
            grant_type);

    auto req = http::request::make("POST", get_token_host(), get_token_path());
    req.write_body("", std::move(body));
    req.set_mime_type(sstring("application/x-www-form-urlencoded"));

    auto factory = std::make_unique<utils::http::dns_connection_factory>(get_token_host(), 443, true, azcredlog);
    http::experimental::client http_client(std::move(factory), 1, http::experimental::client::retry_requests::yes);
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            auto s = co_await util::read_entire_stream_contiguous(lin);
            azcredlog.trace("Got response {}: {}", int(rep._status), s);
            token = { rjson::parse(s), resource_uri };
        }, http::reply::status_type::ok
    ).finally([&] -> future<> { co_await http_client.close(); });
}

future<> service_principal_credentials::refresh_with_certificate(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}