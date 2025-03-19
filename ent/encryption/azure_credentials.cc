/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>

#include "azure_credentials.hh"

namespace azure {

access_token::access_token(const rjson::value& json, const scopes_type& scopes)
    : token(rjson::get<std::string>(json, "access_token"))
    , expiry(timeout_clock::now() + std::chrono::seconds(rjson::get<int>(json, "expires_in")))
    , scopes(scopes)
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

future<access_token> credentials::get_access_token(const scopes_type& scope) {
    if (token.expired() || token.scopes != scope) {
        co_await refresh(scope);
    }
    co_return token;
}

service_principal_credentials::service_principal_credentials(const sstring& tenant_id, const sstring& client_id, const sstring& client_secret, const sstring& client_cert)
        : _tenant_id(tenant_id)
        , _client_id(client_id)
        , _client_secret(client_secret)
        , _client_cert(client_cert)
{}

future<> service_principal_credentials::refresh(const scopes_type& scope) {
    throw std::logic_error("Not implemented");
}

future<> managed_identity_credentials::refresh(const scopes_type& scope) {
    throw std::logic_error("Not implemented");
}

}