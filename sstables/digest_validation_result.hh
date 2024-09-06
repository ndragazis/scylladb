/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>

#include <seastar/core/sstring.hh>

namespace sstables {

enum class digest_validation_status {
    invalid = 0,
    valid = 1,
    in_progress = 2
};

struct digest_validation_result {
    digest_validation_status status;
    std::optional<seastar::sstring> msg;
};

}