**object_store > http**

# Module: http

## Contents

**Structs**

- [`HttpBuilder`](#httpbuilder) - Configure a connection to a generic HTTP server
- [`HttpStore`](#httpstore) - An [`ObjectStore`] implementation for generic HTTP servers

---

## object_store::http::HttpBuilder

*Struct*

Configure a connection to a generic HTTP server

**Methods:**

- `fn new() -> Self` - Create a new [`HttpBuilder`] with default values.
- `fn with_url<impl Into<String>>(self: Self, url: impl Trait) -> Self` - Set the URL
- `fn with_retry(self: Self, retry_config: RetryConfig) -> Self` - Set the retry configuration
- `fn with_config<impl Into<String>>(self: Self, key: ClientConfigKey, value: impl Trait) -> Self` - Set individual client configuration without overriding the entire config
- `fn with_client_options(self: Self, options: ClientOptions) -> Self` - Sets the client options, overriding any already set
- `fn with_http_connector<C>(self: Self, connector: C) -> Self` - The [`HttpConnector`] to use
- `fn build(self: Self) -> Result<HttpStore>` - Build an [`HttpStore`] with the configured options

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> HttpBuilder`
- **Default**
  - `fn default() -> HttpBuilder`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::http::HttpStore

*Struct*

An [`ObjectStore`] implementation for generic HTTP servers

See [`crate::http`] for more information

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ObjectStore**
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, _location: &'life1 Path, _opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



