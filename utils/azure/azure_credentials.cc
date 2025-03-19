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

}