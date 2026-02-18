**object_store > aws**

# Module: aws

## Contents

**Structs**

- [`AmazonS3`](#amazons3) - Interface for [Amazon S3](https://aws.amazon.com/s3/).

**Type Aliases**

- [`AwsCredentialProvider`](#awscredentialprovider) - [`CredentialProvider`] for [`AmazonS3`]

---

## object_store::aws::AmazonS3

*Struct*

Interface for [Amazon S3](https://aws.amazon.com/s3/).

**Methods:**

- `fn credentials(self: &Self) -> &AwsCredentialProvider` - Returns the [`AwsCredentialProvider`] used by [`AmazonS3`]

**Trait Implementations:**

- **Signer**
  - `fn signed_url(self: &'life0 Self, method: Method, path: &'life1 Path, expires_in: Duration) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Create a URL containing the relevant [AWS SigV4] query parameters that authorize a request
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **ObjectStore**
  - `fn put_opts(self: &'life0 Self, location: &'life1 Path, payload: PutPayload, opts: PutOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_multipart_opts(self: &'life0 Self, location: &'life1 Path, opts: PutMultipartOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_opts(self: &'life0 Self, location: &'life1 Path, options: GetOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete(self: &'life0 Self, location: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn delete_stream<'a>(self: &'a Self, locations: BoxStream<'a, Result<Path>>) -> BoxStream<'a, Result<Path>>`
  - `fn list(self: &Self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_offset(self: &Self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>>`
  - `fn list_with_delimiter(self: &'life0 Self, prefix: Option<&'life1 Path>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn copy_if_not_exists(self: &'life0 Self, from: &'life1 Path, to: &'life2 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Clone**
  - `fn clone(self: &Self) -> AmazonS3`
- **PaginatedListStore**
  - `fn list_paginated(self: &'life0 Self, prefix: Option<&'life1 str>, opts: PaginatedListOptions) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **MultipartStore**
  - `fn create_multipart(self: &'life0 Self, path: &'life1 Path) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn put_part(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, part_idx: usize, data: PutPayload) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn complete_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId, parts: Vec<PartId>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn abort_multipart(self: &'life0 Self, path: &'life1 Path, id: &'life2 MultipartId) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::aws::AwsCredentialProvider

*Type Alias*: `std::sync::Arc<dyn CredentialProvider>`

[`CredentialProvider`] for [`AmazonS3`]



