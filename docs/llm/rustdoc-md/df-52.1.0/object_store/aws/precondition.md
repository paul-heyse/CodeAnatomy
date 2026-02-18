**object_store > aws > precondition**

# Module: aws::precondition

## Contents

**Enums**

- [`S3ConditionalPut`](#s3conditionalput) - Configure how to provide conditional put support for [`AmazonS3`].
- [`S3CopyIfNotExists`](#s3copyifnotexists) - Configure how to provide [`ObjectStore::copy_if_not_exists`] for [`AmazonS3`].

---

## object_store::aws::precondition::S3ConditionalPut

*Enum*

Configure how to provide conditional put support for [`AmazonS3`].

[`AmazonS3`]: super::AmazonS3

**Variants:**
- `ETagMatch` - Some S3-compatible stores, such as Cloudflare R2 and minio support conditional
- `Dynamo(crate::aws::dynamo::DynamoCommit)` - The name of a DynamoDB table to use for coordination
- `Disabled` - Disable `conditional put`

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> S3ConditionalPut`
- **Default**
  - `fn default() -> S3ConditionalPut`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &S3ConditionalPut) -> bool`



## object_store::aws::precondition::S3CopyIfNotExists

*Enum*

Configure how to provide [`ObjectStore::copy_if_not_exists`] for [`AmazonS3`].

[`ObjectStore::copy_if_not_exists`]: crate::ObjectStore::copy_if_not_exists
[`AmazonS3`]: super::AmazonS3

**Variants:**
- `Header(String, String)` - Some S3-compatible stores, such as Cloudflare R2, support copy if not exists
- `HeaderWithStatus(String, String, reqwest::StatusCode)` - The same as [`S3CopyIfNotExists::Header`] but allows custom status code checking, for object stores that return values
- `Multipart` - Native Amazon S3 supports copy if not exists through a multipart upload
- `Dynamo(crate::aws::dynamo::DynamoCommit)` - The name of a DynamoDB table to use for coordination

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> S3CopyIfNotExists`
- **PartialEq**
  - `fn eq(self: &Self, other: &S3CopyIfNotExists) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



