**deltalake_core**

# Module: deltalake_core

## Contents

**Modules**

- [`data_catalog`](#data_catalog) - Catalog abstraction for Delta Table
- [`delta_datafusion`](#delta_datafusion) - Datafusion integration for Delta Table
- [`errors`](#errors) - Exceptions for the deltalake crate
- [`kernel`](#kernel) - Delta Kernel module
- [`logstore`](#logstore) - # DeltaLake storage system
- [`operations`](#operations) - High level operations API to interact with Delta tables
- [`protocol`](#protocol) - Actions included in Delta table transaction logs
- [`table`](#table) - Delta Table read and write implementation
- [`writer`](#writer) - Abstractions and implementations for writing data to delta tables

**Functions**

- [`crate_version`](#crate_version) - Returns Rust core version or custom set client_version such as the py-binding
- [`init_client_version`](#init_client_version)
- [`open_table`](#open_table) - Creates and loads a DeltaTable from the given URL with current metadata.
- [`open_table_with_ds`](#open_table_with_ds) - Creates a DeltaTable from the given URL.
- [`open_table_with_storage_options`](#open_table_with_storage_options) - Same as `open_table`, but also accepts storage options to aid in building the table for a deduced
- [`open_table_with_version`](#open_table_with_version) - Creates a DeltaTable from the given URL and loads it with the metadata from the given version.

---

## deltalake_core::crate_version

*Function*

Returns Rust core version or custom set client_version such as the py-binding

```rust
fn crate_version() -> &'static str
```



## Module: data_catalog

Catalog abstraction for Delta Table



## Module: delta_datafusion

Datafusion integration for Delta Table

Example:

```rust
use std::sync::Arc;
use datafusion::execution::context::SessionContext;

async {
  let mut ctx = SessionContext::new();
  let table = deltalake_core::open_table_with_storage_options(
      url::Url::parse("memory://").unwrap(),
      std::collections::HashMap::new()
  )
      .await
      .unwrap();
  ctx.register_table("demo", table.table_provider().await.unwrap()).unwrap();

  let batches = ctx
      .sql("SELECT * FROM demo").await.unwrap()
      .collect()
      .await.unwrap();
};
```



## Module: errors

Exceptions for the deltalake crate



## deltalake_core::init_client_version

*Function*

```rust
fn init_client_version(version: &str)
```



## Module: kernel

Delta Kernel module

The Kernel module contains all the logic for reading and processing the Delta Lake transaction log.



## Module: logstore

# DeltaLake storage system

Interacting with storage systems is a crucial part of any table format.
On one had the storage abstractions need to provide certain guarantees
(e.g. atomic rename, ...) and meet certain assumptions (e.g. sorted list results)
on the other hand can we exploit our knowledge about the general file layout
and access patterns to optimize our operations in terms of cost and performance.

Two distinct phases are involved in querying a Delta table:
- **Metadata**: Fetching metadata about the table, such as schema, partitioning, and statistics.
- **Data**: Reading and processing data files based on the metadata.

When writing to a table, we see the same phases, just in inverse order:
- **Data**: Writing data files that should become part of the table.
- **Metadata**: Updating table metadata to incorporate updates.

Two main abstractions govern the file operations [`LogStore`] and [`ObjectStore`].

[`LogStore`]s are scoped to individual tables and are responsible for maintaining proper
behaviours and ensuring consistency during the metadata phase. The correctness is predicated
on the atomicity and durability guarantees of the implementation of this interface.

- Atomic visibility: Partial writes must not be visible to readers.
- Mutual exclusion: Only one writer must be able to write to a specific log file.
- Consistent listing: Once a file has been written, any future list files operation must return
  the underlying file system entry must immediately.

<div class="warning">

While most object stores today provide the required guarantees, the specific
locking mechanics are a table level responsibility. Specific implementations may
decide to refer to a central catalog or other mechanisms for coordination.

</div>

[`ObjectStore`]s are responsible for direct interactions with storage systems. Either
during the data phase, where additional requirements are imposed on the storage system,
or by specific LogStore implementations for their internal object store interactions.

## Managing LogStores and ObjectStores.

Aside from very basic implementations (i.e. in-memory and local file system) we rely
on external integrations to provide [`ObjectStore`] and/or [`LogStore`] implementations.

At runtime, deltalake needs to produce appropriate [`ObjectStore`]s to access the files
discovered in a table. This is done via

## Configuration




## deltalake_core::open_table

*Function*

Creates and loads a DeltaTable from the given URL with current metadata.
Infers the storage backend to use from the scheme in the given table URL.

Will fail fast if specified `table_uri` is a local path but doesn't exist.

```rust
fn open_table(table_url: url::Url) -> Result<DeltaTable, DeltaTableError>
```



## deltalake_core::open_table_with_ds

*Function*

Creates a DeltaTable from the given URL.

Loads metadata from the version appropriate based on the given ISO-8601/RFC-3339 timestamp.
Infers the storage backend to use from the scheme in the given table URL.

Will fail fast if specified `table_uri` is a local path but doesn't exist.

```rust
fn open_table_with_ds<impl AsRef<str>>(table_url: url::Url, ds: impl Trait) -> Result<DeltaTable, DeltaTableError>
```



## deltalake_core::open_table_with_storage_options

*Function*

Same as `open_table`, but also accepts storage options to aid in building the table for a deduced
`StorageService`.

Will fail fast if specified `table_uri` is a local path but doesn't exist.

```rust
fn open_table_with_storage_options(table_url: url::Url, storage_options: std::collections::HashMap<String, String>) -> Result<DeltaTable, DeltaTableError>
```



## deltalake_core::open_table_with_version

*Function*

Creates a DeltaTable from the given URL and loads it with the metadata from the given version.
Infers the storage backend to use from the scheme in the given table URL.

Will fail fast if specified `table_uri` is a local path but doesn't exist.

```rust
fn open_table_with_version(table_url: url::Url, version: i64) -> Result<DeltaTable, DeltaTableError>
```



## Module: operations

High level operations API to interact with Delta tables

At the heart of the high level operations APIs is the [`DeltaOps`] struct,
which consumes a [`DeltaTable`] and exposes methods to attain builders for
several high level operations. The specific builder structs allow fine-tuning
the operations' behaviors and will return an updated table potentially in conjunction
with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
if the operation returns data as well.



## Module: protocol

Actions included in Delta table transaction logs



## Module: table

Delta Table read and write implementation



## Module: writer

Abstractions and implementations for writing data to delta tables



