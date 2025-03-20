/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define CPP_JWT_USE_VENDORED_NLOHMANN_JSON
#include <jwt/jwt.hpp>

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "utils/http.hh"
#include "types/types.hh"
#include "azure_credentials.hh"
#include "encryption.hh"

static logger azcredlog("azure_credentials");

namespace azure {

template <typename T>
static T get_with_aliases(const rjson::value& json, std::initializer_list<std::string> keys) {
    for (const auto& key : keys) {
        if (auto value = rjson::get_opt<T>(json, key); value) {
            return *value;
        }
    }
    return T{};
}

access_token::access_token(const rjson::value& json, const resource_type& resource_uri)
    : token(get_with_aliases<std::string>(json, {"access_token", "accessToken"}))
    , expiry(timeout_clock::now() + std::chrono::seconds(get_with_aliases<int>(json, {"expires_in", "expires_on"})))
    , resource_uri(resource_uri)
{}

bool access_token::empty() const {
    return token.empty();
}

bool access_token::expired() const {
    if (empty()) {
        return true;
    }
    return timeout_clock::now() >= this->expiry;
}

future<access_token> credentials::get_access_token(const resource_type& resource_uri) {
    if (token.expired() || token.resource_uri != resource_uri) {
        co_await refresh(resource_uri);
    }
    co_return token;
}

service_principal_credentials::service_principal_credentials(const sstring& tenant_id, const sstring& client_id, const sstring& client_secret, const sstring& client_cert)
        : _tenant_id(tenant_id)
        , _client_id(client_id)
        , _client_secret(client_secret)
        , _client_cert(client_cert)
{}

sstring service_principal_credentials::get_token_host() {
    return AZURE_ENTRA_ID_HOST;
}

sstring service_principal_credentials::get_token_path() {
    return seastar::format("/{}/oauth2/v2.0/token", _tenant_id);
}

future<> service_principal_credentials::refresh(const resource_type& resource_uri) {
    if (_client_secret != "") {
        co_await refresh_with_secret(resource_uri);
    } else {
        co_await refresh_with_certificate(resource_uri);
    }
}

// Token request with secret.
// Based on: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret
future<> service_principal_credentials::refresh_with_secret(const resource_type& resource_uri) {
    // Scopes for the client credentials flow must contain only one resource
    // identifier and only the .default scope.
    auto scope = seastar::format("{}/.default", resource_uri);
    sstring grant_type = "client_credentials";
    sstring body = seastar::format(
            "client_id={}&scope={}&client_secret={}&grant_type={}",
            _client_id,
            seastar::http::internal::url_encode(scope),
            seastar::http::internal::url_encode(_client_secret),
            grant_type);

    auto req = http::request::make("POST", get_token_host(), get_token_path());
    req.write_body("", std::move(body));
    req.set_mime_type(sstring("application/x-www-form-urlencoded"));

    auto factory = std::make_unique<utils::http::dns_connection_factory>(get_token_host(), 443, true, azcredlog);
    http::experimental::client http_client(std::move(factory), 1, http::experimental::client::retry_requests::yes);
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            auto s = co_await util::read_entire_stream_contiguous(lin);
            azcredlog.trace("Got response {}: {}", int(rep._status), s);
            token = { rjson::parse(s), scope };
        }, http::reply::status_type::ok
    );
}

static future<std::string> read_pem_artifact(const sstring& key_file, const sstring& header) {
    auto contents = co_await encryption::read_text_file_fully(key_file);
    std::istringstream stream(std::string(contents.begin(), contents.end()));
    std::string line;
    std::string key_data;
    bool in_key = false;
    while (std::getline(stream, line)) {
        if (line.find(header) != std::string::npos) {
            in_key = !in_key;
            key_data += line + "\n";
        } else if (in_key) {
            key_data += line + "\n";
        }
    }
    co_return key_data;
}

// Argument is expected to be a PEM encoded certificate.
// Thumbprint computed as the base64url-encoded SHA-256 hash of the certificate's DER encoding.
// https://learn.microsoft.com/en-us/entra/identity-platform/certificate-credentials#header
// https://datatracker.ietf.org/doc/html/rfc7517?section-4.9
std::string compute_thumbprint(const std::string& pem_cert) {
    BIO* bio = BIO_new_mem_buf(pem_cert.data(), pem_cert.size());
    if (!bio) {
        on_internal_error(azcredlog, "Error creating BIO object");
    }

    X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!cert) {
        on_internal_error(azcredlog, "Error reading certificate from memory");
    }

    // Convert to DER format
    unsigned char* der = nullptr;
    int der_len = i2d_X509(cert, &der);
    X509_free(cert);
    if (der_len < 0) {
        on_internal_error(azcredlog, "Error converting certificate to DER");
    }

    std::string thumbprint(reinterpret_cast<char*>(der), der_len);
    auto sha = encryption::calculate_sha256(to_bytes(thumbprint), 0, der_len);
    return encryption::base64_encode(sha, 0, sha.size(), encryption::make_url_safe::yes);
}

// Token request with certificate.
// Based on: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#second-case-access-token-request-with-a-certificate
future<> service_principal_credentials::refresh_with_certificate(const resource_type& resource_uri) {
    // Scopes for the client credentials flow must contain only one resource
    // identifier and only the .default scope.
    auto scope = seastar::format("{}/.default", resource_uri);
    sstring grant_type = "client_credentials";
    sstring client_assertion_type = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    std::string private_key = co_await read_pem_artifact(_client_cert, PEM_STRING_PKCS8INF);
    if (private_key.empty()) {
        throw std::invalid_argument(seastar::format("Private key not found in certificate file {}", _client_cert));
    }
    std::string cert = co_await read_pem_artifact(_client_cert, PEM_STRING_X509);
    if (cert.empty()) {
        throw std::invalid_argument(seastar::format("Certificate not found in certificate file {}", _client_cert));
    }
    auto alg = "RS256"; // docs suggest PS256, but it's not supported by jwt-cpp
    auto thumbprint = compute_thumbprint(cert);

    using namespace jwt::params;
    jwt::jwt_object obj{algorithm(alg), secret(private_key), headers({{"x5t#S256", thumbprint }})};

    auto uri = seastar::format("https://login.microsoftonline.com/{}/oauth2/v2.0/token", _tenant_id);
    using jwt_id = utils::tagged_uuid<struct jwt_id_tag>;
    obj.add_claim("aud", uri)
        .add_claim("exp", timeout_clock::now() + std::chrono::minutes(10))
        .add_claim("iss", _client_id)
        .add_claim("jti", jwt_id::create_random_id().to_sstring())
        .add_claim("nbf", timeout_clock::now())
        .add_claim("sub", _client_id)
        .add_claim("iat", timeout_clock::now())
    ;
    auto sign = obj.signature();
    sstring body = seastar::format(
            "client_id={}&scope={}&client_assertion_type={}&client_assertion={}&grant_type={}",
            _client_id,
            seastar::http::internal::url_encode(scope),
            seastar::http::internal::url_encode(client_assertion_type),
            sign,
            grant_type);
    azcredlog.info("request body: {}", body);

    auto req = http::request::make("POST", get_token_host(), get_token_path());
    req.write_body("", std::move(body));
    req.set_mime_type(sstring("application/x-www-form-urlencoded"));

    auto factory = std::make_unique<utils::http::dns_connection_factory>(get_token_host(), 443, true, azcredlog);
    http::experimental::client http_client(std::move(factory), 1, http::experimental::client::retry_requests::yes);
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            auto s = co_await util::read_entire_stream_contiguous(lin);
            azcredlog.trace("Got response {}: {}", int(rep._status), s);
            token = { rjson::parse(s), scope };
        }, http::reply::status_type::ok
    );
}

sstring managed_identity_credentials::get_token_host() {
    return IMDS_HOST;
}

sstring managed_identity_credentials::get_token_path(const resource_type& resource_uri) {
    return seastar::format("/metadata/identity/oauth2/token?api-version=2018-02-01&resource={}", resource_uri);
}

future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    auto req = http::request::make("GET", get_token_host(), get_token_path(resource_uri));
    req._headers["Metadata"] = "true";
    auto factory = std::make_unique<utils::http::dns_connection_factory>(get_token_host(), 80, true, azcredlog);
    http::experimental::client http_client(std::move(factory), 1, http::experimental::client::retry_requests::yes);
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto lin = std::move(in);
            auto s = co_await util::read_entire_stream_contiguous(lin);
            azcredlog.trace("Got response {}: {}", int(rep._status), s);
            token = { rjson::parse(s), resource_uri };
        }, http::reply::status_type::ok
    );
}

}