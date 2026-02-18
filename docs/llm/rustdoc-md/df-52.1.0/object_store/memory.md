**object_store > memory**

# Module: memory

## Contents

**Structs**

- [`InMemory`](#inmemory) - In-memory storage suitable for testing or for opting out of using a cloud

---

## object_store::memory::InMemory

*Struct*

In-memory storage suitable for testing or for opting out of using a cloud
storage provider.

**Methods:**

- `fn new() -> Self` - Create new in-memory storage.
- `fn fork(self: &Self) -> Self` - Creates a fork of the store, with the current content copied into the

**Trait Implementations:**

- **MultipartStore**
  - `fn create_multipart(self: &'life0 Self, _path: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_part(self: &'life0 Self, _path: &'life1 Path, id: &'life2 MultipartId, part_idx: usize, payload: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn complete_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, _parts: Vec<PartId>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn abort_multipart(self: &'life0 Self, _path: &'life1 Path, id: &'life2 MultipartId) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **ObjectStore**
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_ranges(self: &'life0 Self, location: &'life1 Path, ranges: &'life2 [Range<u64>]) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn head(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - The memory implementation returns all results, as opposed to the cloud
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Default**
  - `fn default() -> InMemory`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



