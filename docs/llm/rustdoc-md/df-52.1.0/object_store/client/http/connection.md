**object_store > client > http > connection**

# Module: client::http::connection

## Contents

**Structs**

- [`HttpClient`](#httpclient) - An HTTP client
- [`HttpError`](#httperror) - An HTTP protocol error
- [`ReqwestConnector`](#reqwestconnector) - [`HttpConnector`] using [`reqwest::Client`]
- [`SpawnedReqwestConnector`](#spawnedreqwestconnector) - [`reqwest::Client`] connector that performs all I/O on the provided tokio

**Enums**

- [`HttpErrorKind`](#httperrorkind) - Identifies the kind of [`HttpError`]

**Traits**

- [`HttpConnector`](#httpconnector) - A factory for [`HttpClient`]
- [`HttpService`](#httpservice) - An asynchronous function from a [`HttpRequest`] to a [`HttpResponse`].

---

## object_store::client::http::connection::HttpClient

*Struct*

An HTTP client

**Tuple Struct**: `()`

**Methods:**

- `fn new<impl HttpService + 'static>(service: impl Trait) -> Self` - Create a new [`HttpClient`] from an [`HttpService`]
- `fn execute(self: &Self, request: HttpRequest) -> Result<HttpResponse, HttpError>` - Performs [`HttpRequest`] using this client

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> HttpClient`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::client::http::connection::HttpConnector

*Trait*

A factory for [`HttpClient`]

**Methods:**

- `connect`: Create a new [`HttpClient`] with the provided [`ClientOptions`]



## object_store::client::http::connection::HttpError

*Struct*

An HTTP protocol error

Clients should return this when an HTTP request fails to be completed, e.g. because
of a connection issue. This does **not** include HTTP requests that are return
non 2xx Status Codes, as these should instead be returned as an [`HttpResponse`]
with the appropriate status code set.

**Methods:**

- `fn new<E>(kind: HttpErrorKind, e: E) -> Self` - Create a new [`HttpError`] with the optional status code
- `fn kind(self: &Self) -> HttpErrorKind` - Returns the [`HttpErrorKind`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`



## object_store::client::http::connection::HttpErrorKind

*Enum*

Identifies the kind of [`HttpError`]

This is used, among other things, to determine if a request can be retried

**Variants:**
- `Connect` - An error occurred whilst connecting to the remote
- `Request` - An error occurred whilst making the request
- `Timeout` - Request timed out
- `Interrupted` - The request was aborted
- `Decode` - An error occurred whilst decoding the response
- `Unknown` - An unknown error occurred

**Traits:** Copy, Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> HttpErrorKind`
- **PartialEq**
  - `fn eq(self: &Self, other: &HttpErrorKind) -> bool`



## object_store::client::http::connection::HttpService

*Trait*

An asynchronous function from a [`HttpRequest`] to a [`HttpResponse`].

**Methods:**

- `call`: Perform [`HttpRequest`] returning [`HttpResponse`]



## object_store::client::http::connection::ReqwestConnector

*Struct*

[`HttpConnector`] using [`reqwest::Client`]

**Trait Implementations:**

- **HttpConnector**
  - `fn connect(self: &Self, options: &ClientOptions) -> crate::Result<HttpClient>`
- **Default**
  - `fn default() -> ReqwestConnector`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::client::http::connection::SpawnedReqwestConnector

*Struct*

[`reqwest::Client`] connector that performs all I/O on the provided tokio
[`Runtime`] (thread pool).

This adapter is most useful when you wish to segregate I/O from CPU bound
work that may be happening on the [`Runtime`].

[`Runtime`]: tokio::runtime::Runtime

# Example: Spawning requests on separate runtime

```
# use std::sync::Arc;
# use tokio::runtime::Runtime;
# use object_store::azure::MicrosoftAzureBuilder;
# use object_store::client::SpawnedReqwestConnector;
# use object_store::ObjectStore;
# fn get_io_runtime() -> Runtime {
#   tokio::runtime::Builder::new_current_thread().build().unwrap()
# }
# fn main() -> Result<(), object_store::Error> {
// create a tokio runtime for I/O.
let io_runtime: Runtime = get_io_runtime();
// configure a store using the runtime.
let handle = io_runtime.handle().clone(); // get a handle to the same runtime
let store: Arc<dyn ObjectStore> = Arc::new(
  MicrosoftAzureBuilder::new()
    .with_http_connector(SpawnedReqwestConnector::new(handle))
    .with_container_name("my_container")
    .with_account("my_account")
    .build()?
 );
// any requests made using store will be spawned on the io_runtime
# Ok(())
# }
```

**Methods:**

- `fn new(runtime: Handle) -> Self` - Create a new [`SpawnedReqwestConnector`] with the provided [`Handle`] to

**Trait Implementations:**

- **HttpConnector**
  - `fn connect(self: &Self, options: &ClientOptions) -> crate::Result<HttpClient>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



