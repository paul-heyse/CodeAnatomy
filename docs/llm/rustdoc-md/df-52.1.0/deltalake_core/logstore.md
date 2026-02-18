**deltalake_core > logstore**

# Module: logstore

## Contents

**Modules**

- [`config`](#config) - Configuration for the Delta Log Store.

**Structs**

- [`LogStoreConfig`](#logstoreconfig) - Configuration parameters for a log store

**Enums**

- [`CommitOrBytes`](#commitorbytes) - Holder whether it's tmp_commit path or commit bytes

**Functions**

- [`abort_commit_entry`](#abort_commit_entry) - Default implementation for aborting a commit entry
- [`default_logstore`](#default_logstore) - Return the [DefaultLogStore] implementation with the provided configuration options
- [`extract_version_from_filename`](#extract_version_from_filename) - Extract version from a file name in the delta log
- [`get_actions`](#get_actions) - Reads a commit and gets list of actions
- [`get_latest_version`](#get_latest_version) - Default implementation for retrieving the latest version
- [`logstore_for`](#logstore_for) - Return the [LogStoreRef] for the provided [Url] location
- [`logstore_with`](#logstore_with) - Return the [LogStoreRef] using the given [ObjectStoreRef]
- [`read_commit_entry`](#read_commit_entry) - Read delta log for a specific version
- [`to_uri`](#to_uri) - Join the given `root` [Url] with the [Path] to produce a URI (String) of the two together.
- [`write_commit_entry`](#write_commit_entry) - Default implementation for writing a commit entry

**Traits**

- [`LogStore`](#logstore) - Trait for critical operations required to read and write commit entries in Delta logs.

**Type Aliases**

- [`LogStoreRef`](#logstoreref) - Sharable reference to [`LogStore`]

---

## deltalake_core::logstore::CommitOrBytes

*Enum*

Holder whether it's tmp_commit path or commit bytes

**Variants:**
- `TmpCommit(object_store::path::Path)` - Path of the tmp commit, to be used by logstores which use CopyIfNotExists
- `LogBytes(bytes::Bytes)` - Bytes of the log, to be used by logstoers which use Conditional Put

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CommitOrBytes`



## deltalake_core::logstore::LogStore

*Trait*

Trait for critical operations required to read and write commit entries in Delta logs.

The correctness is predicated on the atomicity and durability guarantees of
the implementation of this interface. Specifically,

- Atomic visibility: Any commit created via `write_commit_entry` must become visible atomically.
- Mutual exclusion: Only one writer must be able to create a commit for a specific version.
- Consistent listing: Once a commit entry for version `v` has been written, any future call to
  `get_latest_version` must return a version >= `v`, i.e. the underlying file system entry must
  become visible immediately.

**Methods:**

- `name`: Return the name of this LogStore implementation
- `refresh`: Trigger sync operation on log store to.
- `read_commit_entry`: Read data for commit entry with the given version.
- `write_commit_entry`: Write list of actions as delta commit entry for given version.
- `abort_commit_entry`: Abort the commit entry for the given version.
- `get_latest_version`: Find latest version currently stored in the delta log.
- `object_store`: Get object store, can pass operation_id for object stores linked to an operation
- `root_object_store`
- `engine`
- `to_uri`: [Path] to Delta log
- `root_url`: Get fully qualified uri for table root
- `log_path`: [Path] to Delta log
- `transaction_url`: Generate the appropriate [Url] to use for executing an operation.
- `is_delta_table_location`: Check if the location is a delta table location
- `config`: Get configuration representing configured log store.
- `object_store_url`: Generate a unique enough url to identify the store in datafusion.



## deltalake_core::logstore::LogStoreConfig

*Struct*

Configuration parameters for a log store

**Methods:**

- `fn new(location: &Url, options: StorageConfig) -> Self`
- `fn location(self: &Self) -> &Url`
- `fn options(self: &Self) -> &StorageConfig`
- `fn decorate_store<T>(self: &Self, store: T, table_root: Option<&url::Url>) -> DeltaResult<Box<dyn ObjectStore>>`
- `fn object_store_factory(self: &Self) -> ObjectStoreFactoryRegistry`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Deserialize**
  - `fn deserialize<D>(deserializer: D) -> Result<Self, <D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> LogStoreConfig`
- **Serialize**
  - `fn serialize<S>(self: &Self, serializer: S) -> Result<<S as >::Ok, <S as >::Error>`



## deltalake_core::logstore::LogStoreRef

*Type Alias*: `std::sync::Arc<dyn LogStore>`

Sharable reference to [`LogStore`]



## deltalake_core::logstore::abort_commit_entry

*Function*

Default implementation for aborting a commit entry

```rust
fn abort_commit_entry(storage: &dyn ObjectStore, _version: i64, tmp_commit: &object_store::path::Path) -> Result<(), crate::kernel::transaction::TransactionError>
```



## Module: config

Configuration for the Delta Log Store.

This module manages the various pieces of configuration for the Delta Log Store.
It provides methods for parsing and updating configuration settings. All configuration
is parsed from String -> String mappings.

Specific pieces of configuration must implement the `TryUpdateKey` trait which
defines how to update internal fields based on key-value pairs.



## deltalake_core::logstore::default_logstore

*Function*

Return the [DefaultLogStore] implementation with the provided configuration options

```rust
fn default_logstore(prefixed_store: ObjectStoreRef, root_store: ObjectStoreRef, location: &url::Url, options: &StorageConfig) -> std::sync::Arc<dyn LogStore>
```



## deltalake_core::logstore::extract_version_from_filename

*Function*

Extract version from a file name in the delta log

```rust
fn extract_version_from_filename(name: &str) -> Option<i64>
```



## deltalake_core::logstore::get_actions

*Function*

Reads a commit and gets list of actions

```rust
fn get_actions(version: i64, commit_log_bytes: &bytes::Bytes) -> Result<Vec<crate::kernel::Action>, crate::DeltaTableError>
```



## deltalake_core::logstore::get_latest_version

*Function*

Default implementation for retrieving the latest version

```rust
fn get_latest_version(log_store: &dyn LogStore, current_version: i64) -> crate::DeltaResult<i64>
```



## deltalake_core::logstore::logstore_for

*Function*

Return the [LogStoreRef] for the provided [Url] location

This will use the built-in process global [crate::storage::ObjectStoreRegistry] by default

```rust
# use deltalake_core::logstore::*;
# use std::collections::HashMap;
# use url::Url;
let location = Url::parse("memory:///").expect("Failed to make location");
let storage_config = StorageConfig::default();
let logstore = logstore_for(&location, storage_config).expect("Failed to get a logstore");
```

```rust
fn logstore_for(location: &url::Url, storage_config: StorageConfig) -> crate::DeltaResult<LogStoreRef>
```



## deltalake_core::logstore::logstore_with

*Function*

Return the [LogStoreRef] using the given [ObjectStoreRef]

```rust
fn logstore_with(root_store: ObjectStoreRef, location: &url::Url, storage_config: StorageConfig) -> crate::DeltaResult<LogStoreRef>
```



## deltalake_core::logstore::read_commit_entry

*Function*

Read delta log for a specific version

```rust
fn read_commit_entry(storage: &dyn ObjectStore, version: i64) -> crate::DeltaResult<Option<bytes::Bytes>>
```



## deltalake_core::logstore::to_uri

*Function*

Join the given `root` [Url] with the [Path] to produce a URI (String) of the two together.

This is largely a convenience function to help with the nuances of empty [Path] and file [Url]s

```rust
fn to_uri(root: &url::Url, location: &object_store::path::Path) -> String
```



## deltalake_core::logstore::write_commit_entry

*Function*

Default implementation for writing a commit entry

```rust
fn write_commit_entry(storage: &dyn ObjectStore, version: i64, tmp_commit: &object_store::path::Path) -> Result<(), crate::kernel::transaction::TransactionError>
```



