**deltalake_core > table > builder**

# Module: table::builder

## Contents

**Structs**

- [`DeltaTableBuilder`](#deltatablebuilder) - builder for configuring a delta table load.
- [`DeltaTableConfig`](#deltatableconfig) - Configuration options for delta table

**Enums**

- [`DeltaTableConfigKey`](#deltatableconfigkey)
- [`DeltaVersion`](#deltaversion) - possible version specifications for loading a delta table

**Functions**

- [`ensure_table_uri`](#ensure_table_uri) - Will return an error if the location is not valid. For example,
- [`parse_table_uri`](#parse_table_uri) - Attempt to create a Url from given table location.

---

## deltalake_core::table::builder::DeltaTableBuilder

*Struct*

builder for configuring a delta table load.

**Methods:**

- `fn from_url(table_url: Url) -> DeltaResult<Self>` - Creates `DeltaTableBuilder` from table URL
- `fn without_files(self: Self) -> Self` - Sets `require_files=false` to the builder
- `fn with_version(self: Self, version: i64) -> Self` - Sets `version` to the builder
- `fn with_log_buffer_size(self: Self, log_buffer_size: usize) -> DeltaResult<Self>` - Sets `log_buffer_size` to the builder
- `fn with_datestring<impl AsRef<str>>(self: Self, date_string: impl Trait) -> DeltaResult<Self>` - specify the timestamp given as ISO-8601/RFC-3339 timestamp
- `fn with_timestamp(self: Self, timestamp: DateTime<Utc>) -> Self` - specify a timestamp
- `fn with_storage_backend(self: Self, root_storage: Arc<DynObjectStore>, location: Url) -> Self` - Set the storage backend.
- `fn with_storage_options(self: Self, storage_options: HashMap<String, String>) -> Self` - Set options used to initialize storage backend
- `fn with_allow_http(self: Self, allow_http: bool) -> Self` - Allows insecure connections via http.
- `fn with_io_runtime(self: Self, io_runtime: IORuntime) -> Self` - Provide a custom runtime handle or runtime config
- `fn storage_options(self: &Self) -> HashMap<String, String>` - Storage options for configuring backend object store
- `fn build_storage(self: &Self) -> DeltaResult<LogStoreRef>` - Build a delta storage backend for the given config
- `fn build(self: Self) -> DeltaResult<DeltaTable>` - Build the [`DeltaTable`] from specified options.
- `fn load(self: Self) -> DeltaResult<DeltaTable>` - Build the [`DeltaTable`] and load its state

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::table::builder::DeltaTableConfig

*Struct*

Configuration options for delta table

**Fields:**
- `require_files: bool` - Indicates whether DeltaTable should track files.
- `log_buffer_size: usize` - Controls how many files to buffer from the commit log when updating the table.
- `log_batch_size: usize` - Control the number of records to read / process from the commit / checkpoint files
- `io_runtime: Option<crate::logstore::storage::IORuntime>` - When a runtime handler is provided, all IO tasks are spawn in that handle

**Trait Implementations:**

- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryUpdateKey**
  - `fn try_update_key(self: & mut Self, key: &str, v: &str) -> crate::DeltaResult<Option<()>>`
  - `fn load_from_environment(self: & mut Self) -> crate::DeltaResult<()>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaTableConfig`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **FromIterator**
  - `fn from_iter<I>(iter: I) -> Self`



## deltalake_core::table::builder::DeltaTableConfigKey

*Enum*

**Variants:**
- `RequireFiles` - Indicates whether DeltaTable should track files.
- `LogBufferSize` - Controls how many files to buffer from the commit log when updating the table.
- `LogBatchSize` - Control the number of records to read / process from the commit / checkpoint files
- `IoRuntime` - When a runtime handler is provided, all IO tasks are spawn in that handle



## deltalake_core::table::builder::DeltaVersion

*Enum*

possible version specifications for loading a delta table

**Variants:**
- `Newest` - load the newest version
- `Version(i64)` - specify the version to load
- `Timestamp(chrono::DateTime<chrono::Utc>)` - specify the timestamp in UTC

**Traits:** Copy, Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DeltaVersion`
- **PartialEq**
  - `fn eq(self: &Self, other: &DeltaVersion) -> bool`
- **Default**
  - `fn default() -> DeltaVersion`



## deltalake_core::table::builder::ensure_table_uri

*Function*

Will return an error if the location is not valid. For example,
Creates directories for local paths if they don't exist.

```rust
fn ensure_table_uri<impl AsRef<str>>(table_uri: impl Trait) -> crate::DeltaResult<url::Url>
```



## deltalake_core::table::builder::parse_table_uri

*Function*

Attempt to create a Url from given table location.

The location could be:
 * A valid URL, which will be parsed and returned
 * A path to a directory, which will be created and then converted to a URL.

If it is a local path, it will be created if it doesn't exist.

Extra slashes will be removed from the end path as well.

Parse a table URI to a URL without creating directories.
This is useful for opening existing tables where we don't want to create directories.

```rust
fn parse_table_uri<impl AsRef<str>>(table_uri: impl Trait) -> crate::DeltaResult<url::Url>
```



