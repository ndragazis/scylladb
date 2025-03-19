/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/util/log.hh>
#include <seastar/core/coroutine.hh>

#include "azure_credentials_detector.hh"

static logger azlog("azure_credentials_detector");

namespace azure {

future<credentials_detector::credentials_opt> credentials_detector::detect(source_set sources) {
    if (sources.contains<source::Env>()) {
        azlog.debug("Detecting credentials in environment");
        if (auto creds = co_await get_credentials_from_env()) {
            azlog.debug("Credentials found in environment!");
            co_return creds;
        }
    }
    if (sources.contains<source::AzureCli>()) {
        azlog.debug("Detecting credentials in CLI");
        if (auto creds = co_await get_credentials_from_azure_cli()) {
            azlog.debug("Credentials found in CLI!");
            co_return creds;
        }
    }
    if (sources.contains<source::Imds>()) {
        azlog.debug("Detecting credentials in IMDS");
        if (auto creds = co_await get_credentials_from_imds()) {
            azlog.debug("Credentials found in IMDS!");
            co_return creds;
        }
    }
    azlog.debug("No credentials found in any source.");
    co_return std::nullopt;
}

future<credentials_detector::credentials_opt> credentials_detector::get_credentials_from_env() {
    co_return std::nullopt;
}

future<credentials_detector::credentials_opt> credentials_detector::get_credentials_from_azure_cli() {
    co_return std::nullopt;
}

future<credentials_detector::credentials_opt> credentials_detector::get_credentials_from_imds() {
    co_return std::nullopt;
}

}