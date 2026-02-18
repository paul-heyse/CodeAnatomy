**object_store > throttle**

# Module: throttle

## Contents

**Structs**

- [`ThrottleConfig`](#throttleconfig) - Configuration settings for throttled store
- [`ThrottledStore`](#throttledstore) - Store wrapper that wraps an inner store with some `sleep` calls.

---

## object_store::throttle::ThrottleConfig

*Struct*

Configuration settings for throttled store

**Fields:**
- `wait_delete_per_call: std::time::Duration` - Sleep duration for every call to [`delete`](ThrottledStore::delete).
- `wait_get_per_byte: std::time::Duration` - Sleep duration for every byte received during [`get`](ThrottledStore::get).
- `wait_get_per_call: std::time::Duration` - Sleep duration for every call to [`get`](ThrottledStore::get).
- `wait_list_per_call: std::time::Duration` - Sleep duration for every call to [`list`](ThrottledStore::list).
- `wait_list_per_entry: std::time::Duration` - Sleep duration for every entry received during [`list`](ThrottledStore::list).
- `wait_list_with_delimiter_per_call: std::time::Duration` - Sleep duration for every call to
- `wait_list_with_delimiter_per_entry: std::time::Duration` - Sleep duration for every entry received during
- `wait_put_per_call: std::time::Duration` - Sleep duration for every call to [`put`](ThrottledStore::put).

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ThrottleConfig`
- **Default**
  - `fn default() -> ThrottleConfig`



## object_store::throttle::ThrottledStore

*Struct*

Store wrapper that wraps an inner store with some `sleep` calls.

This can be used for performance testing.

**Note that the behavior of the wrapper is deterministic and might not reflect real-world
conditions!**

**Generic Parameters:**
- T

**Methods:**

- `fn new(inner: T, config: ThrottleConfig) -> Self` - Create new wrapper with zero waiting times.
- `fn config_mut<F>(self: &Self, f: F)` - Mutate config.
- `fn config(self: &Self) -> ThrottleConfig` - Return copy of current config.

**Trait Implementations:**

- **MultipartStore**
  - `fn create_multipart(self: &'life0 Self, path: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_part(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, part_idx: usize, data: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn complete_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, parts: Vec<PartId>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn abort_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **ObjectStore**
  - `fn put(self: &'life0 Self, location: &'life1 Path, payload: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_range(self: &'life0 Self, location: &'life1 Path, range: Range<u64>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_ranges(self: &'life0 Self, location: &'life1 Path, ranges: &'life2 [Range<u64>]) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn head(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_offset(self: &Self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn rename(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn rename_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



