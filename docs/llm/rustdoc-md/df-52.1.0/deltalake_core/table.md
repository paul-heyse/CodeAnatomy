**deltalake_core > table**

# Module: table

## Contents

**Modules**

- [`builder`](#builder) - Create or load DeltaTables
- [`config`](#config) - Delta Table configuration
- [`state`](#state) - The module for delta table state.

**Structs**

- [`DeltaTable`](#deltatable) - In memory representation of a Delta Table

**Functions**

- [`normalize_table_url`](#normalize_table_url) - Normalize a given [Url] to **always** contain a trailing slash. This is critically important

---

## deltalake_core::table::DeltaTable

*Struct*

In memory representation of a Delta Table

A DeltaTable is a purely logical concept that represents a dataset that can ewvolve over time.
To attain concrete information about a table a snapshot need to be loaded.
Most commonly this is the latest state of the tablem but may also loaded for a specific
version or point in time.

**Fields:**
- `state: Option<self::state::DeltaTableState>` - The state of the table as of the most recent loaded Delta log entry.
- `config: self::builder::DeltaTableConfig` - the load options used during load

**Methods:**

- `fn table_provider(self: &Self) -> TableProviderBuilder` - Get a table provider for the table referenced by this DeltaTable.
- `fn update_datafusion_session(self: &Self, session: &dyn Session) -> DeltaResult<()>` - Ensure the provided DataFusion session is prepared to read this table.
- `fn try_from_url(uri: Url) -> DeltaResult<Self>` - Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given URL.
- `fn try_from_url_with_storage_options(uri: Url, storage_options: HashMap<String, String>) -> DeltaResult<Self>` - Create a [`DeltaTable`] instance from URL with storage options
- `fn create(self: &Self) -> CreateBuilder`
- `fn restore(self: Self) -> RestoreBuilder`
- `fn vacuum(self: Self) -> VacuumBuilder` - Vacuum stale files from delta table
- `fn filesystem_check(self: Self) -> FileSystemCheckBuilder` - Audit active files with files present on the filesystem
- `fn add_feature(self: Self) -> AddTableFeatureBuilder` - Enable a table feature for a table
- `fn set_tbl_properties(self: Self) -> SetTablePropertiesBuilder` - Set table properties
- `fn add_columns(self: Self) -> AddColumnBuilder` - Add new columns
- `fn update_field_metadata(self: Self) -> UpdateFieldMetadataBuilder` - Update field metadata
- `fn update_table_metadata(self: Self) -> UpdateTableMetadataBuilder` - Update table metadata
- `fn generate(self: Self) -> GenerateBuilder` - Generate a symlink_format_manifest for other engines
- `fn new(log_store: LogStoreRef, config: DeltaTableConfig) -> Self` - Create a new Delta Table struct without loading any data from backing storage.
- `fn new_in_memory() -> Self` - Create a new [`DeltaTable`] instance, backed by an un-initialized in memory table
- `fn object_store(self: &Self) -> ObjectStoreRef` - get a shared reference to the delta object store
- `fn verify_deltatable_existence(self: &Self) -> DeltaResult<bool>` - Check if the [`DeltaTable`] exists
- `fn table_url(self: &Self) -> &Url` - The URI of the underlying data
- `fn log_store(self: &Self) -> LogStoreRef` - get a shared reference to the log store
- `fn get_latest_version(self: &Self) -> Result<i64, DeltaTableError>` - returns the latest available version of the table
- `fn version(self: &Self) -> Option<i64>` - Currently loaded version of the table - if any.
- `fn load(self: & mut Self) -> Result<(), DeltaTableError>` - Load DeltaTable with data from latest checkpoint
- `fn update_state(self: & mut Self) -> Result<(), DeltaTableError>` - Updates the DeltaTable to the most recent state committed to the transaction log by
- `fn update_incremental(self: & mut Self, max_version: Option<i64>) -> Result<(), DeltaTableError>` - Updates the DeltaTable to the latest version by incrementally applying newer versions.
- `fn load_version(self: & mut Self, version: i64) -> Result<(), DeltaTableError>` - Loads the DeltaTable state for the given version.
- `fn history(self: &Self, limit: Option<usize>) -> Result<impl Trait, DeltaTableError>` - Returns provenance information, including the operation, user, and so on, for each write to a table.
- `fn get_active_add_actions_by_partitions(self: &Self, filters: &[PartitionFilter]) -> BoxStream<DeltaResult<LogicalFileView>>` - Stream all logical files matching the provided `PartitionFilter`s.
- `fn get_files_by_partitions(self: &Self, filters: &[PartitionFilter]) -> Result<Vec<Path>, DeltaTableError>` - Returns the file list tracked in current table state filtered by provided
- `fn get_file_uris_by_partitions(self: &Self, filters: &[PartitionFilter]) -> Result<Vec<String>, DeltaTableError>` - Return the file uris as strings for the partition(s)
- `fn get_file_uris(self: &Self) -> DeltaResult<impl Trait>` - Returns a URIs for all active files present in the current table version.
- `fn snapshot(self: &Self) -> DeltaResult<&DeltaTableState>` - Returns the currently loaded state snapshot.
- `fn load_with_datetime(self: & mut Self, datetime: DateTime<Utc>) -> Result<(), DeltaTableError>` - Time travel Delta table to the latest version that's created at or before provided
- `fn scan_table(self: &Self) -> LoadBuilder`
- `fn scan_cdf(self: Self) -> CdfLoadBuilder` - Load a table with CDF Enabled
- `fn write<impl IntoIterator<Item = RecordBatch>>(self: Self, batches: impl Trait) -> WriteBuilder`
- `fn optimize<'a>(self: Self) -> OptimizeBuilder<'a>` - Audit active files with files present on the filesystem
- `fn delete(self: Self) -> DeleteBuilder` - Delete data from Delta table
- `fn update(self: Self) -> UpdateBuilder` - Update data from Delta table
- `fn merge<E>(self: Self, source: datafusion::prelude::DataFrame, predicate: E) -> MergeBuilder` - Update data from Delta table
- `fn add_constraint(self: Self) -> ConstraintBuilder` - Add a check constraint to a table
- `fn drop_constraints(self: Self) -> DropConstraintBuilder` - Drops constraints from a table

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> Result<(), std::fmt::Error>`
- **From**
  - `fn from(ops: DeltaOps) -> Self`
- **Deserialize**
  - `fn deserialize<D>(deserializer: D) -> Result<Self, <D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaTable`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Serialize**
  - `fn serialize<S>(self: &Self, serializer: S) -> Result<<S as >::Ok, <S as >::Error>`



## Module: builder

Create or load DeltaTables



## Module: config

Delta Table configuration



## deltalake_core::table::normalize_table_url

*Function*

Normalize a given [Url] to **always** contain a trailing slash. This is critically important
for assumptions about [Url] equivalency and more importantly for **joining** on a Url`.

This function will also remove redundant slashes in the ]Url] path which can cause other
equivalency failures

```ignore
 left.join("_delta_log"); // produces `s3://bucket/prefix/_delta_log`
 right.join("_delta_log"); // produces `s3://bucket/_delta_log`
```

```rust
fn normalize_table_url(url: &url::Url) -> url::Url
```



## Module: state

The module for delta table state.



