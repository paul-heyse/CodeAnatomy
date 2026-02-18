**object_store**

# Module: object_store

## Contents

**Modules**

- [`aws`](#aws) - An object store implementation for S3
- [`azure`](#azure) - An object store implementation for Azure blob storage
- [`buffered`](#buffered) - Utilities for performing tokio-style buffered IO
- [`chunked`](#chunked) - A [`ChunkedStore`] that can be used to test streaming behaviour
- [`client`](#client) - Generic utilities for [`reqwest`] based [`ObjectStore`] implementations
- [`delimited`](#delimited) - Utility for streaming newline delimited files from object storage
- [`gcp`](#gcp) - An object store implementation for Google Cloud Storage
- [`http`](#http) - An object store implementation for generic HTTP servers
- [`limit`](#limit) - An object store that limits the maximum concurrency of the wrapped implementation
- [`list`](#list) - Paginated Listing
- [`local`](#local) - An object store implementation for a local filesystem
- [`memory`](#memory) - An in-memory object store implementation
- [`multipart`](#multipart) - Cloud Multipart Upload
- [`path`](#path) - Path abstraction for Object Storage
- [`prefix`](#prefix) - An object store wrapper handling a constant path prefix
- [`registry`](#registry) - Map object URLs to [`ObjectStore`]
- [`signer`](#signer) - Abstraction of signed URL generation for those object store implementations that support it
- [`throttle`](#throttle) - A throttling object store wrapper

**Structs**

- [`GetOptions`](#getoptions) - Options for a get request, such as range
- [`GetResult`](#getresult) - Result for a get request
- [`ListResult`](#listresult) - Result of a list call that includes objects, prefixes (directories) and a
- [`ObjectMeta`](#objectmeta) - The metadata that describes an object.
- [`PutMultipartOptions`](#putmultipartoptions) - Options for [`ObjectStore::put_multipart_opts`]
- [`PutOptions`](#putoptions) - Options for a put request
- [`PutResult`](#putresult) - Result for a put request
- [`UpdateVersion`](#updateversion) - Uniquely identifies a version of an object to update

**Enums**

- [`Error`](#error) - A specialized `Error` for object store-related errors
- [`GetResultPayload`](#getresultpayload) - The kind of a [`GetResult`]
- [`PutMode`](#putmode) - Configure preconditions for the put operation

**Traits**

- [`ObjectStore`](#objectstore) - Universal API to multiple object store services.

**Type Aliases**

- [`DynObjectStore`](#dynobjectstore) - An alias for a dynamically dispatched object store implementation.
- [`MultipartId`](#multipartid) - Id type for multipart uploads.
- [`Result`](#result) - A specialized `Result` for object store-related errors

---

## object_store::DynObjectStore

*Type Alias*: `dyn ObjectStore`

An alias for a dynamically dispatched object store implementation.



## object_store::Error

*Enum*

A specialized `Error` for object store-related errors

**Variants:**
- `Generic{ store: &'static str, source: Box<dyn std::error::Error> }` - A fallback error type when no variant matches
- `NotFound{ path: String, source: Box<dyn std::error::Error> }` - Error when the object is not found at given location
- `InvalidPath{ source: path::Error }` - Error for invalid path
- `JoinError{ source: tokio::task::JoinError }` - Error when `tokio::spawn` failed
- `NotSupported{ source: Box<dyn std::error::Error> }` - Error when the attempted operation is not supported
- `AlreadyExists{ path: String, source: Box<dyn std::error::Error> }` - Error when the object already exists
- `Precondition{ path: String, source: Box<dyn std::error::Error> }` - Error when the required conditions failed for the operation
- `NotModified{ path: String, source: Box<dyn std::error::Error> }` - Error when the object at the location isn't modified
- `NotImplemented` - Error when an operation is not implemented
- `PermissionDenied{ path: String, source: Box<dyn std::error::Error> }` - Error when the used credentials don't have enough permission
- `Unauthenticated{ path: String, source: Box<dyn std::error::Error> }` - Error when the used credentials lack valid authentication
- `UnknownConfigurationKey{ store: &'static str, key: String }` - Error when a configuration key is invalid for the store used

**Trait Implementations:**

- **From**
  - `fn from(source: path::Error) -> Self`
- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(source: tokio::task::JoinError) -> Self`



## object_store::GetOptions

*Struct*

Options for a get request, such as range

**Fields:**
- `if_match: Option<String>` - Request will succeed if the `ObjectMeta::e_tag` matches
- `if_none_match: Option<String>` - Request will succeed if the `ObjectMeta::e_tag` does not match
- `if_modified_since: Option<chrono::DateTime<chrono::Utc>>` - Request will succeed if the object has been modified since
- `if_unmodified_since: Option<chrono::DateTime<chrono::Utc>>` - Request will succeed if the object has not been modified since
- `range: Option<GetRange>` - Request transfer of only the specified range of bytes
- `version: Option<String>` - Request a particular object version
- `head: bool` - Request transfer of no content
- `extensions: Extensions` - Implementation-specific extensions. Intended for use by [`ObjectStore`] implementations

**Methods:**

- `fn check_preconditions(self: &Self, meta: &ObjectMeta) -> Result<()>` - Returns an error if the modification conditions on this request are not satisfied

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GetOptions`
- **Default**
  - `fn default() -> GetOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::GetResult

*Struct*

Result for a get request

**Fields:**
- `payload: GetResultPayload` - The [`GetResultPayload`]
- `meta: ObjectMeta` - The [`ObjectMeta`] for this object
- `range: std::ops::Range<u64>` - The range of bytes returned by this request
- `attributes: Attributes` - Additional object attributes

**Methods:**

- `fn bytes(self: Self) -> Result<Bytes>` - Collects the data into a [`Bytes`]
- `fn into_stream(self: Self) -> BoxStream<'static, Result<Bytes>>` - Converts this into a byte stream

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::GetResultPayload

*Enum*

The kind of a [`GetResult`]

This special cases the case of a local file, as some systems may
be able to optimise the case of a file already present on local disk

**Variants:**
- `File(std::fs::File, std::path::PathBuf)` - The file, path
- `Stream(futures::stream::BoxStream<'static, Result<bytes::Bytes>>)` - An opaque stream of bytes

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`



## object_store::ListResult

*Struct*

Result of a list call that includes objects, prefixes (directories) and a
token for the next set of results. Individual result sets may be limited to
1,000 objects based on the underlying object storage's limitations.

**Fields:**
- `common_prefixes: Vec<crate::path::Path>` - Prefixes that are common (like directories)
- `objects: Vec<ObjectMeta>` - Object metadata for the listing

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::MultipartId

*Type Alias*: `String`

Id type for multipart uploads.



## object_store::ObjectMeta

*Struct*

The metadata that describes an object.

**Fields:**
- `location: crate::path::Path` - The full path to the object
- `last_modified: chrono::DateTime<chrono::Utc>` - The last modified time
- `size: u64` - The size in bytes of the object.
- `e_tag: Option<String>` - The unique identifier for the object
- `version: Option<String>` - A version indicator for this object

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ObjectMeta`
- **PartialEq**
  - `fn eq(self: &Self, other: &ObjectMeta) -> bool`



## object_store::ObjectStore

*Trait*

Universal API to multiple object store services.

**Methods:**

- `put`: Save the provided bytes to the specified location
- `put_opts`: Save the provided `payload` to `location` with the given options
- `put_multipart`: Perform a multipart upload
- `put_multipart_opts`: Perform a multipart upload with options
- `get`: Return the bytes that are stored at the specified location.
- `get_opts`: Perform a get request with options
- `get_range`: Return the bytes that are stored at the specified location
- `get_ranges`: Return the bytes that are stored at the specified location
- `head`: Return the metadata for the specified location
- `delete`: Delete the object at the specified location.
- `delete_stream`: Delete all the objects at the specified locations
- `list`: List all the objects with the given prefix.
- `list_with_offset`: List all the objects with the given prefix and a location greater than `offset`
- `list_with_delimiter`: List objects with the given prefix and an implementation specific
- `copy`: Copy an object from one path to another in the same object store.
- `rename`: Move an object from one path to another in the same object store.
- `copy_if_not_exists`: Copy an object from one path to another, only if destination is empty.
- `rename_if_not_exists`: Move an object from one path to another in the same object store.



## object_store::PutMode

*Enum*

Configure preconditions for the put operation

**Variants:**
- `Overwrite` - Perform an atomic write operation, overwriting any object present at the provided path
- `Create` - Perform an atomic write operation, returning [`Error::AlreadyExists`] if an
- `Update(UpdateVersion)` - Perform an atomic write operation if the current version of the object matches the

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PutMode`
- **PartialEq**
  - `fn eq(self: &Self, other: &PutMode) -> bool`
- **Default**
  - `fn default() -> PutMode`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::PutMultipartOptions

*Struct*

Options for [`ObjectStore::put_multipart_opts`]

**Fields:**
- `tags: TagSet` - Provide a [`TagSet`] for this object
- `attributes: Attributes` - Provide a set of [`Attributes`]
- `extensions: Extensions` - Implementation-specific extensions. Intended for use by [`ObjectStore`] implementations

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> PutMultipartOptions`
- **From**
  - `fn from(tags: TagSet) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> PutMultipartOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **From**
  - `fn from(attributes: Attributes) -> Self`



## object_store::PutOptions

*Struct*

Options for a put request

**Fields:**
- `mode: PutMode` - Configure the [`PutMode`] for this operation
- `tags: TagSet` - Provide a [`TagSet`] for this object
- `attributes: Attributes` - Provide a set of [`Attributes`]
- `extensions: Extensions` - Implementation-specific extensions. Intended for use by [`ObjectStore`] implementations

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(mode: PutMode) -> Self`
- **From**
  - `fn from(attributes: Attributes) -> Self`
- **Default**
  - `fn default() -> PutOptions`
- **Clone**
  - `fn clone(self: &Self) -> PutOptions`
- **From**
  - `fn from(tags: TagSet) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`



## object_store::PutResult

*Struct*

Result for a put request

**Fields:**
- `e_tag: Option<String>` - The unique identifier for the newly created object
- `version: Option<String>` - A version indicator for the newly created object

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PutResult`
- **PartialEq**
  - `fn eq(self: &Self, other: &PutResult) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::Result

*Type Alias*: `std::result::Result<T, E>`

A specialized `Result` for object store-related errors



## object_store::UpdateVersion

*Struct*

Uniquely identifies a version of an object to update

Stores will use differing combinations of `e_tag` and `version` to provide conditional
updates, and it is therefore recommended applications preserve both

**Fields:**
- `e_tag: Option<String>` - The unique identifier for the newly created object
- `version: Option<String>` - A version indicator for the newly created object

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(value: PutResult) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> UpdateVersion`
- **PartialEq**
  - `fn eq(self: &Self, other: &UpdateVersion) -> bool`



## Module: aws

An object store implementation for S3

## Multipart uploads

Multipart uploads can be initiated with the [ObjectStore::put_multipart] method.

If the writer fails for any reason, you may have parts uploaded to AWS but not
used that you will be charged for. [`MultipartUpload::abort`] may be invoked to drop
these unneeded parts, however, it is recommended that you consider implementing
[automatic cleanup] of unused parts that are older than some threshold.

[automatic cleanup]: https://aws.amazon.com/blogs/aws/s3-lifecycle-management-update-support-for-multipart-uploads-and-delete-markers/



## Module: azure

An object store implementation for Azure blob storage

## Streaming uploads

[ObjectStore::put_multipart] will upload data in blocks and write a blob from those blocks.

Unused blocks will automatically be dropped after 7 days.



## Module: buffered

Utilities for performing tokio-style buffered IO



## Module: chunked

A [`ChunkedStore`] that can be used to test streaming behaviour



## Module: client

Generic utilities for [`reqwest`] based [`ObjectStore`] implementations

[`ObjectStore`]: crate::ObjectStore



## Module: delimited

Utility for streaming newline delimited files from object storage



## Module: gcp

An object store implementation for Google Cloud Storage

## Multipart uploads

[Multipart uploads](https://cloud.google.com/storage/docs/multipart-uploads)
can be initiated with the [ObjectStore::put_multipart] method. If neither
[`MultipartUpload::complete`] nor [`MultipartUpload::abort`] is invoked, you may
have parts uploaded to GCS but not used, that you will be charged for. It is recommended
you configure a [lifecycle rule] to abort incomplete multipart uploads after a certain
period of time to avoid being charged for storing partial uploads.

## Using HTTP/2

Google Cloud Storage supports both HTTP/2 and HTTP/1. HTTP/1 is used by default
because it allows much higher throughput in our benchmarks (see
[#5194](https://github.com/apache/arrow-rs/issues/5194)). HTTP/2 can be
enabled by setting [crate::ClientConfigKey::Http1Only] to false.

[lifecycle rule]: https://cloud.google.com/storage/docs/lifecycle#abort-mpu



## Module: http

An object store implementation for generic HTTP servers

This follows [rfc2518] commonly known as [WebDAV]

Basic get support will work out of the box with most HTTP servers,
even those that don't explicitly support [rfc2518]

Other operations such as list, delete, copy, etc... will likely
require server-side configuration. A list of HTTP servers with support
can be found [here](https://wiki.archlinux.org/title/WebDAV#Server)

Multipart uploads are not currently supported

[rfc2518]: https://datatracker.ietf.org/doc/html/rfc2518
[WebDAV]: https://en.wikipedia.org/wiki/WebDAV



## Module: limit

An object store that limits the maximum concurrency of the wrapped implementation



## Module: list

Paginated Listing



## Module: local

An object store implementation for a local filesystem



## Module: memory

An in-memory object store implementation



## Module: multipart

Cloud Multipart Upload

This crate provides an asynchronous interface for multipart file uploads to
cloud storage services. It's designed to offer efficient, non-blocking operations,
especially useful when dealing with large files or high-throughput systems.



## Module: path

Path abstraction for Object Storage



## Module: prefix

An object store wrapper handling a constant path prefix



## Module: registry

Map object URLs to [`ObjectStore`]



## Module: signer

Abstraction of signed URL generation for those object store implementations that support it



## Module: throttle

A throttling object store wrapper



