**object_store > azure**

# Module: azure

## Contents

**Structs**

- [`MicrosoftAzure`](#microsoftazure) - Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).

**Type Aliases**

- [`AzureCredentialProvider`](#azurecredentialprovider) - [`CredentialProvider`] for [`MicrosoftAzure`]

---

## object_store::azure::AzureCredentialProvider

*Type Alias*: `std::sync::Arc<dyn CredentialProvider>`

[`CredentialProvider`] for [`MicrosoftAzure`]



## object_store::azure::MicrosoftAzure

*Struct*

Interface for [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).

**Methods:**

- `fn credentials(self: &Self) -> &AzureCredentialProvider` - Returns the [`AzureCredentialProvider`] used by [`MicrosoftAzure`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PaginatedListStore**
  - `fn list_paginated(self: &'life0 Self, prefix: Option<&'life1 str>, opts: PaginatedListOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **MultipartStore**
  - `fn create_multipart(self: &'life0 Self, _: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_part(self: &'life0 Self, path: &'life1 Path, _: &'life2 MultipartId, part_idx: usize, data: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn complete_multipart(self: &'life0 Self, path: &'life1 Path, _: &'life2 MultipartId, parts: Vec<PartId>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn abort_multipart(self: &'life0 Self, _: &'life1 Path, _: &'life2 MultipartId) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **ObjectStore**
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn delete_stream<'a>(self: &'a Self, locations: BoxStream<'a, Result<Path>>) -> BoxStream<'a, Result<Path>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Signer**
  - `fn signed_url(self: &'life0 Self, method: Method, path: &'life1 Path, expires_in: Duration) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Create a URL containing the relevant [Service SAS] query parameters that authorize a request
  - `fn signed_urls(self: &'life0 Self, method: Method, paths: &'life1 [Path], expires_in: Duration) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



