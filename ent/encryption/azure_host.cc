/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <stdexcept>

#include "azure_host.hh"

namespace encryption {

class azure_host::impl {
public:
    impl(const host_options& options);
    future<> init();
    future<key_and_id_type> get_or_create_key(const key_info&);
    future<key_ptr> get_key_by_id(const id_type&, const key_info&);
private:
    const host_options _options;
};

azure_host::impl::impl(const host_options&) {}

future<> azure_host::impl::init() {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_and_id_type> azure_host::impl::get_or_create_key(const key_info& info) {
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