**object_store > parse**

# Module: parse

## Contents

**Enums**

- [`Error`](#error)
- [`ObjectStoreScheme`](#objectstorescheme) - Recognizes various URL formats, identifying the relevant [`ObjectStore`]

**Functions**

- [`parse_url`](#parse_url) - Create an [`ObjectStore`] based on the provided `url`
- [`parse_url_opts`](#parse_url_opts) - Create an [`ObjectStore`] based on the provided `url` and options

---

## object_store::parse::Error

*Enum*

**Variants:**
- `Unrecognised{ url: url::Url }`
- `Path{ source: crate::path::Error }`



## object_store::parse::ObjectStoreScheme

*Enum*

Recognizes various URL formats, identifying the relevant [`ObjectStore`]

See [`ObjectStoreScheme::parse`] for more details

# Supported formats:
- `file:///path/to/my/file` -> [`LocalFileSystem`]
- `memory:///` -> [`InMemory`]
- `s3://bucket/path` -> [`AmazonS3`](crate::aws::AmazonS3) (also supports `s3a`)
- `gs://bucket/path` -> [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
- `az://account/container/path` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure) (also supports `adl`, `azure`, `abfs`, `abfss`)
- `http://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)
- `https://mydomain/path` -> [`HttpStore`](crate::http::HttpStore)

There are also special cases for AWS and Azure for `https://{host?}/path` paths:
- `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, `blob.fabric.microsoft.com` -> [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
- `amazonaws.com` -> [`AmazonS3`](crate::aws::AmazonS3)
- `r2.cloudflarestorage.com` -> [`AmazonS3`](crate::aws::AmazonS3)


**Variants:**
- `Local` - Url corresponding to [`LocalFileSystem`]
- `Memory` - Url corresponding to [`InMemory`]
- `AmazonS3` - Url corresponding to [`AmazonS3`](crate::aws::AmazonS3)
- `GoogleCloudStorage` - Url corresponding to [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
- `MicrosoftAzure` - Url corresponding to [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
- `Http` - Url corresponding to [`HttpStore`](crate::http::HttpStore)

**Methods:**

- `fn parse(url: &Url) -> Result<(Self, Path), Error>` - Create an [`ObjectStoreScheme`] from the provided [`Url`]

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ObjectStoreScheme`
- **PartialEq**
  - `fn eq(self: &Self, other: &ObjectStoreScheme) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::parse::parse_url

*Function*

Create an [`ObjectStore`] based on the provided `url`

Returns
- An [`ObjectStore`] of the corresponding type
- The [`Path`] into the [`ObjectStore`] of the addressed resource

```rust
fn parse_url(url: &url::Url) -> Result<(Box<dyn ObjectStore>, crate::path::Path), super::Error>
```



## object_store::parse::parse_url_opts

*Function*

Create an [`ObjectStore`] based on the provided `url` and options

This method can be used to create an instance of one of the provided
`ObjectStore` implementations based on the URL scheme (see
[`ObjectStoreScheme`] for more details).

For example
* `file:///path/to/my/file` will return a [`LocalFileSystem`] instance
* `s3://bucket/path` will return an [`AmazonS3`] instance if the `aws` feature is enabled.

Arguments:
* `url`: The URL to parse
* `options`: A list of key-value pairs to pass to the [`ObjectStore`] builder.
  Note different object stores accept different configuration options, so
  the options that are read depends on the `url` value. One common pattern
  is to pass configuration information via process variables using
  [`std::env::vars`].

Returns
- An [`ObjectStore`] of the corresponding type
- The [`Path`] into the [`ObjectStore`] of the addressed resource

[`AmazonS3`]: https://docs.rs/object_store/0.12.0/object_store/aws/struct.AmazonS3.html

```rust
fn parse_url_opts<I, K, V>(url: &url::Url, options: I) -> Result<(Box<dyn ObjectStore>, crate::path::Path), super::Error>
```



