**object_store > gcp**

# Module: gcp

## Contents

**Structs**

- [`GoogleCloudStorage`](#googlecloudstorage) - Interface for [Google Cloud Storage](https://cloud.google.com/storage/).

**Type Aliases**

- [`GcpCredentialProvider`](#gcpcredentialprovider) - [`CredentialProvider`] for [`GoogleCloudStorage`]
- [`GcpSigningCredentialProvider`](#gcpsigningcredentialprovider) - [`GcpSigningCredential`] for [`GoogleCloudStorage`]

---

## object_store::gcp::GcpCredentialProvider

*Type Alias*: `std::sync::Arc<dyn CredentialProvider>`

[`CredentialProvider`] for [`GoogleCloudStorage`]



## object_store::gcp::GcpSigningCredentialProvider

*Type Alias*: `std::sync::Arc<dyn CredentialProvider>`

[`GcpSigningCredential`] for [`GoogleCloudStorage`]



## object_store::gcp::GoogleCloudStorage

*Struct*

Interface for [Google Cloud Storage](https://cloud.google.com/storage/).

**Methods:**

- `fn credentials(self: &Self) -> &GcpCredentialProvider` - Returns the [`GcpCredentialProvider`] used by [`GoogleCloudStorage`]
- `fn signing_credentials(self: &Self) -> &GcpSigningCredentialProvider` - Returns the [`GcpSigningCredentialProvider`] used by [`GoogleCloudStorage`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **MultipartStore**
  - `fn create_multipart(self: &'life0 Self, path: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_part(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, part_idx: usize, payload: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn complete_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, parts: Vec<PartId>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn abort_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **ObjectStore**
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_offset(self: &Self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PaginatedListStore**
  - `fn list_paginated(self: &'life0 Self, prefix: Option<&'life1 str>, opts: PaginatedListOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Clone**
  - `fn clone(self: &Self) -> GoogleCloudStorage`
- **Signer**
  - `fn signed_url(self: &'life0 Self, method: Method, path: &'life1 Path, expires_in: Duration) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



