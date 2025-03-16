/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "symmetric_key.hh"
#include "azure_credentials.hh"

namespace encryption {

class azure_host {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    using id_type = bytes;
    using key_ptr = shared_ptr<symmetric_key>;
    using key_and_id_type = std::tuple<key_ptr, id_type>;

    struct host_options {
        std::string tenant_id;
        std::string client_id;
        std::string client_secret;
        std::string client_cert;

        // Azure Key Vault Key to encrypt data keys with. Format: <vault name>/<keyname>
        std::string master_key;

        // TLS options
        std::string truststore;
        std::string priority_string;

        std::optional<std::chrono::milliseconds> key_cache_expiry;
        std::optional<std::chrono::milliseconds> key_cache_refresh;

        std::unique_ptr<azure::credentials> get_credentials() const {
            if (!tenant_id.empty() && !client_id.empty() && (!client_secret.empty() || !client_cert.empty())) {
                return std::make_unique<azure::service_principal_credentials>(
                    tenant_id,
                    client_id,
                    client_secret,
                    client_cert);
            }
            return {};
        }
    };

    azure_host(const host_options&);
    azure_host(const std::unordered_map<sstring, sstring>&);

    future<> init();
    future<key_and_id_type> get_or_create_key(const key_info&);
    future<key_ptr> get_key_by_id(const id_type&, const key_info&);
};

}