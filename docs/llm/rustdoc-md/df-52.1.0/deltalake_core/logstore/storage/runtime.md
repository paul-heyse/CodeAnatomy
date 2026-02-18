**deltalake_core > logstore > storage > runtime**

# Module: logstore::storage::runtime

## Contents

**Structs**

- [`DeltaIOStorageBackend`](#deltaiostoragebackend) - Wraps any object store and runs IO in it's own runtime [EXPERIMENTAL]
- [`RuntimeConfig`](#runtimeconfig) - Configuration for Tokio runtime

**Enums**

- [`IORuntime`](#ioruntime) - Provide custom Tokio RT or a runtime config

---

## deltalake_core::logstore::storage::runtime::DeltaIOStorageBackend

*Struct*

Wraps any object store and runs IO in it's own runtime [EXPERIMENTAL]

**Generic Parameters:**
- T

**Fields:**
- `inner: T`

**Methods:**

- `fn spawn_io_rt<F, O>(self: &Self, f: F, store: &T, path: Path) -> BoxFuture<ObjectStoreResult<O>>` - spawn tasks on IO runtime
- `fn spawn_io_rt_from_to<F, O>(self: &Self, f: F, store: &T, from: Path, to: Path) -> BoxFuture<ObjectStoreResult<O>>` - spawn tasks on IO runtime
- `fn new(store: T, rt: IORuntime) -> Self`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DeltaIOStorageBackend<T>`
- **Debug**
  - `fn fmt(self: &Self, fmt: & mut std::fmt::Formatter) -> Result<(), std::fmt::Error>`
- **ObjectStore**
  - `fn put(self: &'life0 Self, location: &'life1 Path, bytes: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, bytes: PutPayload, options: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_range(self: &'life0 Self, location: &'life1 Path, range: Range<u64>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn head(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>>`
  - `fn list_with_offset(self: &Self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn rename_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, options: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Display**
  - `fn fmt(self: &Self, fmt: & mut std::fmt::Formatter) -> Result<(), std::fmt::Error>`



## deltalake_core::logstore::storage::runtime::IORuntime

*Enum*

Provide custom Tokio RT or a runtime config

**Variants:**
- `RT(tokio::runtime::Handle)` - Tokio RT handle
- `Config(RuntimeConfig)` - Configuration for tokio runtime

**Methods:**

- `fn get_handle(self: &Self) -> Handle` - Retrieves the Tokio runtime for IO bound operations

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> IORuntime`



## deltalake_core::logstore::storage::runtime::RuntimeConfig

*Struct*

Configuration for Tokio runtime



