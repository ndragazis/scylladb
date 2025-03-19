/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <optional>

#include <seastar/core/future.hh>

#include "enum_set.hh"
#include "azure_credentials.hh"

namespace azure {

class credentials_detector {
public:
    enum class source : uint8_t {
        Env,
        AzureCli,
        Imds,
    };
    using source_set = enum_set<super_enum<source,
        source::Env,
        source::AzureCli,
        source::Imds>>;
    static constexpr source_set all_sources = source_set::full();

    using credentials_opt = std::optional<std::unique_ptr<credentials>>;
    static future<credentials_opt> detect(source_set sources = all_sources);
private:
    static future<credentials_opt> get_credentials_from_env();
    static future<credentials_opt> get_credentials_from_azure_cli();
    static future<credentials_opt> get_credentials_from_imds();
};

}