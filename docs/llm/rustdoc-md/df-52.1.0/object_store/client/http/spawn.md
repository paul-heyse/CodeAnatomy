**object_store > client > http > spawn**

# Module: client::http::spawn

## Contents

**Structs**

- [`SpawnService`](#spawnservice) - Wraps a provided [`HttpService`] and runs it on a separate tokio runtime

---

## object_store::client::http::spawn::SpawnService

*Struct*

Wraps a provided [`HttpService`] and runs it on a separate tokio runtime

See example on [`SpawnedReqwestConnector`]

[`SpawnedReqwestConnector`]: crate::client::http::SpawnedReqwestConnector

**Generic Parameters:**
- T

**Methods:**

- `fn new(inner: T, runtime: Handle) -> Self` - Creates a new [`SpawnService`] from the provided

**Trait Implementations:**

- **HttpService**
  - `fn call(self: &'life0 Self, req: HttpRequest) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



