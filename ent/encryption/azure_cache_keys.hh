/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "utils/hash.hh"
#include "symmetric_key.hh"

namespace encryption {

struct attr_cache_key {
    seastar::sstring master_key;
    key_info info;
    bool operator==(const attr_cache_key& v) const = default;
};

struct attr_cache_key_hash {
    size_t operator()(const attr_cache_key& k) const {
        return utils::tuple_hash()(std::tie(k.master_key, k.info.len));
    }
};

}

template<>
struct fmt::formatter<encryption::attr_cache_key> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const encryption::attr_cache_key& d, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{},{}", d.master_key, d.info.len);
    }
};