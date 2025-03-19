/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <stdexcept>

#include <seastar/http/request.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/short_streams.hh>

#include <boost/regex.hpp>

#include "utils/log.hh"
#include "utils/http.hh"
#include "utils/rjson.hh"
#include "utils/loading_cache.hh"
#include "azure_host.hh"
#include "azure_cache_keys.hh"
#include "encryption_exceptions.hh"

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
    std::unique_ptr<azure::credentials> _credentials;

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
    cache_type<id_cache_key, bytes, id_cache_key_hash> _id_cache;

    static constexpr char AKV_HOST_TEMPLATE[] = "{}.vault.azure.net";
    static constexpr char AKV_PATH_TEMPLATE[] = "/keys/{}/{}/{}?api-version=7.4";
    static constexpr char AKV_LATEST_VERSION[] = "";
    static constexpr char AKV_WRAPKEY_OP[] = "wrapkey";
    static constexpr char AKV_TOKEN_ALG[] = "RSA-OAEP-256";
    static constexpr char AKV_TOKEN_SCOPE[] = "https://vault.azure.net/.default";

    static std::tuple<std::string, std::string> parse_key(std::string_view);
    future<rjson::value> send_request(const sstring& host, const sstring& path, const rjson::value& body);
    future<key_and_id_type> create_key(const attr_cache_key&);
    future<bytes> find_key(const id_cache_key&);
};

azure_host::impl::impl(const azure_host::host_options& options)
    : _options(options)
    , _credentials(options.get_credentials())
    , _attr_cache(utils::loading_cache_config{
        .max_size = std::numeric_limits<size_t>::max(),
        .expiry = options.key_cache_expiry.value_or(default_expiry),
        .refresh = options.key_cache_refresh.value_or(default_refresh)}, azlog, std::bind_front(&impl::create_key, this))
    , _id_cache(utils::loading_cache_config{
        .max_size = std::numeric_limits<size_t>::max(),
        .expiry = options.key_cache_expiry.value_or(default_expiry),
        .refresh = options.key_cache_refresh.value_or(default_refresh)}, azlog, std::bind_front(&impl::find_key, this))
{}

future<> azure_host::impl::init() {
    throw std::logic_error("Not implemented");
}

future<azure_host::key_and_id_type> azure_host::impl::get_or_create_key(const key_info& info) {
    attr_cache_key key {
        .master_key = _options.master_key,
        .info = info,
    };

    if (key.master_key.empty()) {
        throw configuration_error("No master key set in azure host config");
    }
    try {
        co_return co_await _attr_cache.get(key);
    } catch (base_error&) {
        throw;
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("get_or_create_key: {}", e.what())));
    } catch (rjson::malformed_value& e) {
        std::throw_with_nested(malformed_response_error(fmt::format("get_or_create_key: {}", e.what())));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_or_create_key: {}", std::current_exception())));
    }
}

future<azure_host::key_ptr> azure_host::impl::get_key_by_id(const azure_host::id_type& id, const key_info& info) {
    throw std::logic_error("Not implemented");
}

std::tuple<std::string, std::string> azure_host::impl::parse_key(std::string_view spec) {
    auto i = spec.find_last_of('/');
    if (i == std::string_view::npos) {
        throw std::invalid_argument(fmt::format("Invalid master key spec '{}'. Must be in format <vaultname>/<keyname>", spec));
    }
    return std::make_tuple(std::string(spec.substr(0, i)), std::string(spec.substr(i + 1)));
}

future<rjson::value> azure_host::impl::send_request(const sstring& host, const sstring& path, const rjson::value& body) {
    // Audience must be "cfa8b339-82a2-471a-a3c9-0fc0be7a4093".
    // https://learn.microsoft.com/en-us/azure/key-vault/secrets/overview-storage-keys#service-principal-application-id
    // https://github.com/pulumi/pulumi-azure-native/issues/2432
    auto token = co_await _credentials->get_access_token(AKV_TOKEN_SCOPE);
    auto req = http::request::make("POST", host, path);
    req._headers["Authorization"] = fmt::format("Bearer {}", token.token);
    req.write_body("application/json", std::move(rjson::print(body)));
    req.set_mime_type(sstring("application/json"));

    auto addr = co_await net::dns::resolve_name(host, net::inet_address::family::INET);

    auto certs = seastar::make_shared<tls::certificate_credentials>();
    //co_await certs->set_system_trust();
    // FIXME: Set the trust store to the system trust store. `set_system_trust()` doesn't work.
    co_await certs->set_x509_trust_file("/etc/ssl/certs/ca-certificates.crt", seastar::tls::x509_crt_format::PEM);
    http::experimental::client http_client(socket_address(addr, 443), std::move(certs), host);
    rjson::value j;
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            auto s = co_await util::read_entire_stream_contiguous(lin);
            azlog.trace("Got response {}: {}", int(rep._status), s);
            j = rjson::parse(s);
        }, http::reply::status_type::ok
    );
    co_return j;
}

future<azure_host::key_and_id_type> azure_host::impl::create_key(const attr_cache_key& k) {
    auto& info = k.info;
    azlog.debug("Creating new key: {}", info);
    auto [vault, keyname] = parse_key(k.master_key);
    auto key = make_shared<symmetric_key>(info);
    auto host = fmt::format(AKV_HOST_TEMPLATE, vault);
    auto path = fmt::format(AKV_PATH_TEMPLATE, keyname, AKV_LATEST_VERSION, AKV_WRAPKEY_OP);
    auto body = [&key] {
        auto b = rjson::empty_object();
        rjson::add(b, "alg", AKV_TOKEN_ALG);
        rjson::add(b, "value", std::string(base64_encode(key->key())));
        return b;
    }();
    auto resp = co_await send_request(host, path, body);
    auto key_id = rjson::get<std::string>(resp, "kid");
    auto cipher = rjson::get<std::string>(resp, "value");
    boost::regex version_regex(R"foo(.*/([^/]+)$)foo");
    boost::smatch match;
    if (!boost::regex_search(key_id, match, version_regex)) {
        throw std::runtime_error(fmt::format("Failed to parse key version from key id {}", key_id));
    }
    auto key_version = match[1].str();

    auto sid = fmt::format("{}/{}/{}:{}", vault, keyname, key_version, cipher);
    bytes id(sid.begin(), sid.end());

    azlog.trace("Created key id {}", sid);
    co_return key_and_id_type{ key, id };
}

future<bytes> azure_host::impl::find_key(const id_cache_key& k) {
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