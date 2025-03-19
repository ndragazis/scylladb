/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <stdexcept>

#include "utils/log.hh"
#include "utils/loading_cache.hh"
#include "azure_host.hh"
#include "azure_cache_keys.hh"

using namespace std::chrono_literals;

static logging::logger azlog("azure");

namespace encryption {

class azure_host::impl {
public:
    static inline constexpr std::chrono::milliseconds default_expiry = 600s;
    static inline constexpr std::chrono::milliseconds default_refresh = 1200s;
    impl(const host_options& options);
    future<> init();
    future<key_and_id_type> get_or_create_key(const key_info&);
    future<key_ptr> get_key_by_id(const id_type&, const key_info&);
private:
    const host_options _options;

    template<typename Key, typename Value, typename Hash>
    using cache_type = utils::loading_cache<
        Key,
        Value,
        2,
        utils::loading_cache_reload_enabled::yes,
        utils::simple_entry_size<Value>,
        Hash
    >;
    cache_type<attr_cache_key, key_and_id_type, attr_cache_key_hash> _attr_cache;

    future<key_and_id_type> create_key(const attr_cache_key&);
};

azure_host::impl::impl(const azure_host::host_options& options)
    : _options(options)
    , _attr_cache(utils::loading_cache_config{
        .max_size = std::numeric_limits<size_t>::max(),
        .expiry = options.key_cache_expiry.value_or(default_expiry),
        .refresh = options.key_cache_refresh.value_or(default_refresh)}, azlog, std::bind_front(&impl::create_key, this))
{}

future<> azure_host::impl::init() {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_and_id_type> azure_host::impl::get_or_create_key(const key_info& info) {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_and_id_type> azure_host::impl::create_key(const attr_cache_key& key) {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_ptr> azure_host::impl::get_key_by_id(const azure_host::id_type& id, const key_info& info) {
    throw std::logic_error("Not implemented");
}

// ==================== azure_host class implementation ====================

azure_host::azure_host(const host_options& options) : _impl(std::make_unique<impl>(options)) {}

future<> azure_host::init() {
    return _impl->init();
}

future<azure_host::key_and_id_type> azure_host::get_or_create_key(const key_info& info) {
    return _impl->get_or_create_key(info);
}

future<azure_host::key_ptr> azure_host::get_key_by_id(const azure_host::id_type& id, const key_info& info) {
    return _impl->get_key_by_id(id, info);
}

}