**object_store > client**

# Module: client

## Contents

**Structs**

- [`Certificate`](#certificate) - Represents a CA certificate provided by the user.
- [`ClientOptions`](#clientoptions) - HTTP client configuration for remote object stores
- [`StaticCredentialProvider`](#staticcredentialprovider) - A static set of credentials

**Enums**

- [`ClientConfigKey`](#clientconfigkey) - Configuration keys for [`ClientOptions`]

**Traits**

- [`CredentialProvider`](#credentialprovider) - Provides credentials for use when signing requests

---

## object_store::client::Certificate

*Struct*

Represents a CA certificate provided by the user.

This is used to configure the client to trust a specific certificate. See
[Self::from_pem] for an example

**Tuple Struct**: `()`

**Methods:**

- `fn from_pem(pem: &[u8]) -> Result<Self>` - Create a `Certificate` from a PEM encoded certificate.
- `fn from_pem_bundle(pem_bundle: &[u8]) -> Result<Vec<Self>>` - Create a collection of `Certificate` from a PEM encoded certificate
- `fn from_der(der: &[u8]) -> Result<Self>` - Create a `Certificate` from a binary DER encoded certificate.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Certificate`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::client::ClientConfigKey

*Enum*

Configuration keys for [`ClientOptions`]

**Variants:**
- `AllowHttp` - Allow non-TLS, i.e. non-HTTPS connections
- `AllowInvalidCertificates` - Skip certificate validation on https connections.
- `ConnectTimeout` - Timeout for only the connect phase of a Client
- `DefaultContentType` - default CONTENT_TYPE for uploads
- `Http1Only` - Only use http1 connections
- `Http2KeepAliveInterval` - Interval for HTTP2 Ping frames should be sent to keep a connection alive.
- `Http2KeepAliveTimeout` - Timeout for receiving an acknowledgement of the keep-alive ping.
- `Http2KeepAliveWhileIdle` - Enable HTTP2 keep alive pings for idle connections
- `Http2MaxFrameSize` - Sets the maximum frame size to use for HTTP2.
- `Http2Only` - Only use http2 connections
- `PoolIdleTimeout` - The pool max idle timeout
- `PoolMaxIdlePerHost` - maximum number of idle connections per host
- `ProxyUrl` - HTTP proxy to use for requests
- `ProxyCaCertificate` - PEM-formatted CA certificate for proxy connections
- `ProxyExcludes` - List of hosts that bypass proxy
- `RandomizeAddresses` - Randomize order addresses that the DNS resolution yields.
- `Timeout` - Request timeout
- `UserAgent` - User-Agent header to be used by this client

**Traits:** Copy, Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> ClientConfigKey`
- **PartialEq**
  - `fn eq(self: &Self, other: &ClientConfigKey) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## object_store::client::ClientOptions

*Struct*

HTTP client configuration for remote object stores

**Methods:**

- `fn new() -> Self` - Create a new [`ClientOptions`] with default values
- `fn with_config<impl Into<String>>(self: Self, key: ClientConfigKey, value: impl Trait) -> Self` - Set an option by key
- `fn get_config_value(self: &Self, key: &ClientConfigKey) -> Option<String>` - Get an option by key
- `fn with_user_agent(self: Self, agent: HeaderValue) -> Self` - Sets the User-Agent header to be used by this client
- `fn with_root_certificate(self: Self, certificate: Certificate) -> Self` - Add a custom root certificate.
- `fn with_default_content_type<impl Into<String>>(self: Self, mime: impl Trait) -> Self` - Set the default CONTENT_TYPE for uploads
- `fn with_content_type_for_suffix<impl Into<String>, impl Into<String>>(self: Self, extension: impl Trait, mime: impl Trait) -> Self` - Set the CONTENT_TYPE for a given file extension
- `fn with_default_headers(self: Self, headers: HeaderMap) -> Self` - Sets the default headers for every request
- `fn with_allow_http(self: Self, allow_http: bool) -> Self` - Sets what protocol is allowed. If `allow_http` is :
- `fn with_allow_invalid_certificates(self: Self, allow_insecure: bool) -> Self` - Allows connections to invalid SSL certificates
- `fn with_http1_only(self: Self) -> Self` - Only use http1 connections
- `fn with_http2_only(self: Self) -> Self` - Only use http2 connections
- `fn with_allow_http2(self: Self) -> Self` - Use http2 if supported, otherwise use http1.
- `fn with_proxy_url<impl Into<String>>(self: Self, proxy_url: impl Trait) -> Self` - Set a proxy URL to use for requests
- `fn with_proxy_ca_certificate<impl Into<String>>(self: Self, proxy_ca_certificate: impl Trait) -> Self` - Set a trusted proxy CA certificate
- `fn with_proxy_excludes<impl Into<String>>(self: Self, proxy_excludes: impl Trait) -> Self` - Set a list of hosts to exclude from proxy connections
- `fn with_timeout(self: Self, timeout: Duration) -> Self` - Set timeout for the overall request
- `fn with_timeout_disabled(self: Self) -> Self` - Disables the request timeout
- `fn with_connect_timeout(self: Self, timeout: Duration) -> Self` - Set a timeout for only the connect phase of a Client
- `fn with_connect_timeout_disabled(self: Self) -> Self` - Disables the connection timeout
- `fn with_pool_idle_timeout(self: Self, timeout: Duration) -> Self` - Set the pool max idle timeout
- `fn with_pool_max_idle_per_host(self: Self, max: usize) -> Self` - Set the maximum number of idle connections per host
- `fn with_http2_keep_alive_interval(self: Self, interval: Duration) -> Self` - Sets an interval for HTTP2 Ping frames should be sent to keep a connection alive.
- `fn with_http2_keep_alive_timeout(self: Self, interval: Duration) -> Self` - Sets a timeout for receiving an acknowledgement of the keep-alive ping.
- `fn with_http2_keep_alive_while_idle(self: Self) -> Self` - Enable HTTP2 keep alive pings for idle connections
- `fn with_http2_max_frame_size(self: Self, sz: u32) -> Self` - Sets the maximum frame size to use for HTTP2.
- `fn get_content_type(self: &Self, path: &Path) -> Option<&str>` - Get the mime type for the file in `path` to be uploaded

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ClientOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



## object_store::client::CredentialProvider

*Trait*

Provides credentials for use when signing requests

**Methods:**

- `Credential`: The type of credential returned by this provider
- `get_credential`: Return a credential



## object_store::client::StaticCredentialProvider

*Struct*

A static set of credentials

**Generic Parameters:**
- T

**Methods:**

- `fn new(credential: T) -> Self` - A [`CredentialProvider`] for a static credential of type `T`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **CredentialProvider**
  - `fn get_credential(self: &'life0 Self) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



