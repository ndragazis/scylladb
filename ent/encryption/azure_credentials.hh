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
using scopes_type = std::string;

struct access_token {
    sstring token;
    timestamp_type expiry;
    scopes_type scopes;

    access_token() = default;
    access_token(const rjson::value&, const scopes_type& scopes);

    bool empty() const;
    bool expired() const;
};

class credentials {
protected:
    access_token token;
private:
    virtual future<> refresh(const scopes_type& scope) = 0;
public:
    virtual ~credentials() = default;
    future<access_token> get_access_token(const scopes_type& scope);
};

}