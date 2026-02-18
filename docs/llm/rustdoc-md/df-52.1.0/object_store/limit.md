**object_store > limit**

# Module: limit

## Contents

**Structs**

- [`LimitStore`](#limitstore) - Store wrapper that wraps an inner store and limits the maximum number of concurrent
- [`LimitUpload`](#limitupload) - An [`MultipartUpload`] wrapper that limits the maximum number of concurrent requests

---

## object_store::limit::LimitStore

*Struct*

Store wrapper that wraps an inner store and limits the maximum number of concurrent
object store operations. Where each call to an [`ObjectStore`] member function is
considered a single operation, even if it may result in more than one network call

```
# use object_store::memory::InMemory;
# use object_store::limit::LimitStore;

// Create an in-memory `ObjectStore` limited to 20 concurrent requests
let store = LimitStore::new(InMemory::new(), 20);
```


**Generic Parameters:**
- T

**Methods:**

- `fn new(inner: T, max_requests: usize) -> Self` - Create new limit store that will limit the maximum

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
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
  - `fn delete_stream<'a>(self: &'a Self, locations: BoxStream<'a, Result<Path>>) -> BoxStream<'a, Result<Path>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_offset(self: &Self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn rename(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn rename_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::limit::LimitUpload

*Struct*

An [`MultipartUpload`] wrapper that limits the maximum number of concurrent requests

**Methods:**

- `fn new(upload: Box<dyn MultipartUpload>, max_concurrency: usize) -> Self` - Create a new [`LimitUpload`] limiting `upload` to `max_concurrency` concurrent requests

**Trait Implementations:**

- **MultipartUpload**
  - `fn put_part(self: & mut Self, data: PutPayload) -> UploadPart`
  - `fn complete(self: &'life0  mut Self) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn abort(self: &'life0  mut Self) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



