/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <chrono>

#include "utils/rjson.hh"

namespace azure {

using timeout_clock = std::chrono::system_clock;
using timestamp_type = typename timeout_clock::time_point;
using resource_type = std::string;

struct access_token {
    sstring token;
    timestamp_type expiry;
    resource_type resource_uri;

    access_token() = default;
    access_token(const rjson::value&, const resource_type& resource_uri);

    bool empty() const;
    bool expired() const;
};

class credentials {
protected:
    access_token token;
private:
    virtual future<> refresh(const resource_type& resource_uri) = 0;
public:
    virtual ~credentials() = default;
    future<access_token> get_access_token(const resource_type& resource_uri);
};

class service_principal_credentials : public credentials {
    static constexpr char AZURE_ENTRA_ID_HOST[] = "login.microsoftonline.com";
    static constexpr char AZURE_ENTRA_ID_TOKEN_PATH[] = "/oauth2/v2.0/token";

    sstring _tenant_id;
    sstring _client_id;
    sstring _client_secret;
    sstring _client_cert;
public:
    service_principal_credentials(const sstring& tenant_id, const sstring& client_id, const sstring& client_secret, const sstring& client_cert);
    future<> refresh(const resource_type& resource_uri) override;
};

class managed_identity_credentials : public credentials {
    static constexpr char IMDS_INSTANCE_METADATA_URL[] = "http://169.254.169.254/metadata/instance";
public:
    future<> refresh(const resource_type& resource_uri) override;
};

}