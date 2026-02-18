**deltalake_core > operations**

# Module: operations

## Contents

**Modules**

- [`add_column`](#add_column) - Add a new column to a table
- [`add_feature`](#add_feature) - Enable table features
- [`constraints`](#constraints) - Add a check constraint to a table
- [`convert_to_delta`](#convert_to_delta) - Command for converting a Parquet table to a Delta table in place
- [`create`](#create) - Command for creating a new delta table
- [`delete`](#delete) - Delete records from a Delta Table that satisfy a predicate
- [`drop_constraints`](#drop_constraints) - Drop a constraint from a table
- [`filesystem_check`](#filesystem_check) - Audit the Delta Table for active files that do not exist in the underlying filesystem and remove them.
- [`generate`](#generate)
- [`load_cdf`](#load_cdf) - Module for reading the change datafeed of delta tables
- [`merge`](#merge) - Merge data from a source dataset with the target Delta Table based on a join
- [`optimize`](#optimize) - Optimize a Delta Table
- [`restore`](#restore) - Perform restore of delta table to a specified version or datetime
- [`set_tbl_properties`](#set_tbl_properties) - Set table properties on a table
- [`update`](#update) - Update records from a Delta Table for records satisfy a predicate
- [`update_field_metadata`](#update_field_metadata) - Update metadata on a field in a schema
- [`update_table_metadata`](#update_table_metadata) - Update table metadata operation
- [`vacuum`](#vacuum) - Vacuum a Delta table
- [`write`](#write)

**Structs**

- [`DeltaOps`](#deltaops) - High level interface for executing commands against a DeltaTable

**Functions**

- [`get_num_idx_cols_and_stats_columns`](#get_num_idx_cols_and_stats_columns) - Get the num_idx_columns and stats_columns from the table configuration in the state

**Traits**

- [`CustomExecuteHandler`](#customexecutehandler)

---

## deltalake_core::operations::CustomExecuteHandler

*Trait*

**Methods:**

- `pre_execute`
- `post_execute`
- `before_post_commit_hook`
- `after_post_commit_hook`



## deltalake_core::operations::DeltaOps

*Struct*

High level interface for executing commands against a DeltaTable

**Tuple Struct**: `(crate::DeltaTable)`

**Methods:**

- `fn try_from_url(uri: Url) -> DeltaResult<Self>` - Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given URL.
- `fn try_from_url_with_storage_options(uri: Url, storage_options: HashMap<String, String>) -> DeltaResult<Self>` - Create a [`DeltaOps`] instance from URL with storage options
- `fn new_in_memory() -> Self` - Create a new [`DeltaOps`] instance, backed by an un-initialized in memory table
- `fn create(self: Self) -> CreateBuilder` - Create a new Delta table
- `fn generate(self: Self) -> GenerateBuilder` - Generate a symlink_format_manifest for other engines
- `fn load(self: Self) -> LoadBuilder` - Load data from a DeltaTable
- `fn load_cdf(self: Self) -> CdfLoadBuilder` - Load a table with CDF Enabled
- `fn write<impl IntoIterator<Item = RecordBatch>>(self: Self, batches: impl Trait) -> WriteBuilder` - Write data to Delta table
- `fn vacuum(self: Self) -> VacuumBuilder` - Vacuum stale files from delta table
- `fn filesystem_check(self: Self) -> FileSystemCheckBuilder` - Audit and repair active files with files present on the filesystem
- `fn optimize<'a>(self: Self) -> OptimizeBuilder<'a>` - Audit active files with files present on the filesystem
- `fn delete(self: Self) -> DeleteBuilder` - Delete data from Delta table
- `fn update(self: Self) -> UpdateBuilder` - Update data from Delta table
- `fn restore(self: Self) -> RestoreBuilder` - Restore delta table to a specified version or datetime
- `fn merge<E>(self: Self, source: datafusion::prelude::DataFrame, predicate: E) -> MergeBuilder` - Update data from Delta table
- `fn add_constraint(self: Self) -> ConstraintBuilder` - Add a check constraint to a table
- `fn add_feature(self: Self) -> AddTableFeatureBuilder` - Enable a table feature for a table
- `fn drop_constraints(self: Self) -> DropConstraintBuilder` - Drops constraints from a table
- `fn set_tbl_properties(self: Self) -> SetTablePropertiesBuilder` - Set table properties
- `fn add_columns(self: Self) -> AddColumnBuilder` - Add new columns
- `fn update_field_metadata(self: Self) -> UpdateFieldMetadataBuilder` - Update field metadata
- `fn update_table_metadata(self: Self) -> UpdateTableMetadataBuilder` - Update table metadata

**Trait Implementations:**

- **AsRef**
  - `fn as_ref(self: &Self) -> &DeltaTable`
- **From**
  - `fn from(table: DeltaTable) -> Self`



## Module: add_column

Add a new column to a table



## Module: add_feature

Enable table features



## Module: constraints

Add a check constraint to a table



## Module: convert_to_delta

Command for converting a Parquet table to a Delta table in place



## Module: create

Command for creating a new delta table



## Module: delete

Delete records from a Delta Table that satisfy a predicate

When a predicate is not provided then all records are deleted from the Delta
Table. Otherwise a scan of the Delta table is performed to mark any files
that contain records that satisfy the predicate. Once files are determined
they are rewritten without the records.

Predicates MUST be deterministic otherwise undefined behaviour may occur during the
scanning and rewriting phase.

# Example
```
# use datafusion::logical_expr::{col, lit};
# use deltalake_core::{DeltaTable, kernel::{DataType, PrimitiveType, StructType, StructField}};
# use deltalake_core::operations::delete::DeleteBuilder;
# tokio_test::block_on(async {
#  let schema = StructType::try_new(vec![
#      StructField::new(
#          "id".to_string(),
#          DataType::Primitive(PrimitiveType::String),
#          true,
#      )]).expect("Failed to generate schema for test");
# let table = DeltaTable::try_from_url(url::Url::parse("memory://").unwrap())
#               .await.expect("Failed to construct DeltaTable instance for test")
#        .create()
#        .with_columns(schema.fields().cloned())
#        .await
#        .expect("Failed to create test table");
let (table, metrics) = table.delete()
    .with_predicate(col("id").eq(lit(102)))
    .await
    .expect("Failed to delete");
# })
````



## Module: drop_constraints

Drop a constraint from a table



## Module: filesystem_check

Audit the Delta Table for active files that do not exist in the underlying filesystem and remove them.

Active files are ones that have an add action in the log, but no corresponding remove action.
This operation creates a new transaction containing a remove action for each of the missing files.

This can be used to repair tables where a data file has been deleted accidentally or
purposefully, if the file was corrupted.

# Example
```rust ignore
let mut table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let (table, metrics) = FileSystemCheckBuilder::new(table.object_store(), table.state).await?;
````



## Module: generate


The generate supports the fairly simple "GENERATE" operation which produces a
[symlink_format_manifest](https://docs.delta.io/delta-utility/#generate-a-manifest-file) file
when needed for an external engine such as Presto or BigQuery.

The "symlink_format_manifest" is not something that has been well documented, but for
enon-partitioned tables this will generate a `_symlink_format_manifest/manifest` file next to
the `_delta_log`, for example:

```ignore
COVID-19_NYT
├── _delta_log
│   ├── 00000000000000000000.crc
│   └── 00000000000000000000.json
├── part-00000-a496f40c-e091-413a-85f9-b1b69d4b3b4e-c000.snappy.parquet
├── part-00001-9d9d980b-c500-4f0b-bb96-771a515fbccc-c000.snappy.parquet
├── part-00002-8826af84-73bd-49a6-a4b9-e39ffed9c15a-c000.snappy.parquet
├── part-00003-539aff30-2349-4b0d-9726-c18630c6ad90-c000.snappy.parquet
├── part-00004-1bb9c3e3-c5b0-4d60-8420-23261f58a5eb-c000.snappy.parquet
├── part-00005-4d47f8ff-94db-4d32-806c-781a1cf123d2-c000.snappy.parquet
├── part-00006-d0ec7722-b30c-4e1c-92cd-b4fe8d3bb954-c000.snappy.parquet
├── part-00007-4582392f-9fc2-41b0-ba97-a74b3afc8239-c000.snappy.parquet
└── _symlink_format_manifest
    └── manifest
```

For partitioned tables, a `manifest` file will be generated inside a hive-style partitioned
tree structure, e.g.:

```ignore
delta-0.8.0-partitioned
├── _delta_log
│   └── 00000000000000000000.json
├── _symlink_format_manifest
│   ├── year=2020
│   │   ├── month=1
│   │   │   └── day=1
│   │   │       └── manifest
│   │   └── month=2
│   │       ├── day=3
│   │       │   └── manifest
│   │       └── day=5
│   │           └── manifest
│   └── year=2021
│       ├── month=12
│       │   ├── day=20
│       │   │   └── manifest
│       │   └── day=4
│       │       └── manifest
│       └── month=4
│           └── day=5
│               └── manifest
├── year=2020
│   ├── month=1
│   │   └── day=1
│   │       └── part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet
│   └── month=2
│       ├── day=3
│       │   └── part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet
│       └── day=5
│           └── part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet
└── year=2021
    ├── month=12
    │   ├── day=20
    │   │   └── part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet
    │   └── day=4
    │       └── part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet
    └── month=4
        └── day=5
            └── part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet
```



## deltalake_core::operations::get_num_idx_cols_and_stats_columns

*Function*

Get the num_idx_columns and stats_columns from the table configuration in the state
If table_config does not exist (only can occur in the first write action) it takes
the configuration that was passed to the writerBuilder.

```rust
fn get_num_idx_cols_and_stats_columns(config: Option<&delta_kernel::table_properties::TableProperties>, configuration: std::collections::HashMap<String, Option<String>>) -> (delta_kernel::table_properties::DataSkippingNumIndexedCols, Option<Vec<String>>)
```



## Module: load_cdf

Module for reading the change datafeed of delta tables

# Example
```rust ignore
let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let builder = CdfLoadBuilder::new(table.log_store(), table.snapshot())
    .with_starting_version(3);

let ctx = SessionContext::new();
let provider = DeltaCdfTableProvider::try_new(builder)?;
let df = ctx.read_table(provider).await?;



## Module: merge

Merge data from a source dataset with the target Delta Table based on a join
predicate.  A full outer join is performed which results in source and
target records that match, source records that do not match, or target
records that do not match.

Users can specify update, delete, and insert operations for these categories
and specify additional predicates for finer control. The order of operations
specified matter.  See [`MergeBuilder`] for more information

# Example
```rust ignore
let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let (table, metrics) = DeltaOps(table)
    .merge(source, col("target.id").eq(col("source.id")))
    .with_source_alias("source")
    .with_target_alias("target")
    .when_matched_update(|update| {
        update
            .update("value", col("source.value") + lit(1))
            .update("modified", col("source.modified"))
    })?
    .when_not_matched_insert(|insert| {
        insert
            .set("id", col("source.id"))
            .set("value", col("source.value"))
            .set("modified", col("source.modified"))
    })?
    .await?
````



## Module: optimize

Optimize a Delta Table

Perform bin-packing on a Delta Table which merges small files into a large
file. Bin-packing reduces the number of API calls required for read
operations.

Optimize will fail if a concurrent write operation removes files from the
table (such as in an overwrite). It will always succeed if concurrent writers
are only appending.

Optimize increments the table's version and creates remove actions for
optimized files. Optimize does not delete files from storage. To delete
files that were removed, call `vacuum` on [`DeltaTable`].

See [`OptimizeBuilder`] for configuration.

# Example
```rust ignore
let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let (table, metrics) = OptimizeBuilder::new(table.object_store(), table.state).await?;
````



## Module: restore

Perform restore of delta table to a specified version or datetime

Algorithm:
1) Read the latest state snapshot of the table.
2) Read table state for version or datetime to restore
3) Compute files available in state for restoring (files were removed by some commit)
   but missed in the latest. Add these files into commit as AddFile action.
4) Compute files available in the latest state snapshot (files were added after version to restore)
   but missed in the state to restore. Add these files into commit as RemoveFile action.
5) If ignore_missing_files option is false (default value) check availability of AddFile
   in file system.
6) Commit Protocol, all RemoveFile and AddFile actions
   into delta log using `LogStore::write_commit_entry` (commit will be failed in case of parallel transaction)
   TODO: comment is outdated
7) If table was modified in parallel then ignore restore and raise exception.

# Example
```rust ignore
let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let (table, metrics) = RestoreBuilder::new(table.object_store(), table.state).with_version_to_restore(1).await?;
````



## Module: set_tbl_properties

Set table properties on a table



## Module: update

Update records from a Delta Table for records satisfy a predicate

When a predicate is not provided then all records are updated from the Delta
Table. Otherwise a scan of the Delta table is performed to mark any files
that contain records that satisfy the predicate. Once they are determined
then column values are updated with new values provided by the user


Predicates MUST be deterministic otherwise undefined behaviour may occur during the
scanning and rewriting phase.

# Example
```rust ignore
let table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let (table, metrics) = UpdateBuilder::new(table.object_store(), table.state)
    .with_predicate(col("col1").eq(lit(1)))
    .with_update("value", col("value") + lit(20))
    .await?;
````



## Module: update_field_metadata

Update metadata on a field in a schema



## Module: update_table_metadata

Update table metadata operation



## Module: vacuum

Vacuum a Delta table

Run the Vacuum command on the Delta Table: delete files no longer referenced by a Delta table and are older than the retention threshold.
We do not recommend that you set a retention interval shorter than 7 days, because old snapshots
and uncommitted files can still be in use by concurrent readers or writers to the table.

If vacuum cleans up active files, concurrent readers can fail or, worse, tables can be
corrupted when vacuum deletes files that have not yet been committed.
If `retention_period` is not set then the `configuration.deletedFileRetentionDuration` of
delta table is used or if that's missing too, then the default value of 7 days otherwise.

When you run vacuum then you cannot use time travel to a version older than
the specified retention period.

Warning: Vacuum does not support partitioned tables on Windows. This is due
to Windows not using unix style paths. See #682

# Example
```rust ignore
let mut table = open_table(Url::from_directory_path("/abs/path/to/table").unwrap())?;
let (table, metrics) = VacuumBuilder::new(table.object_store(). table.state).await?;
````



## Module: write


New Table Semantics
 - The schema of the [Plan] is used to initialize the table.
 - The partition columns will be used to partition the table.

Existing Table Semantics
 - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
 - Conflicting columns (i.e. a INT, and a STRING)
   will result in an exception.
 - The partition columns, if present, are validated against the existing metadata. If not
   present, then the partitioning of the table is respected.

In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
replace data that matches a predicate.

# Example
```rust ignore
let id_field = arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false);
let schema = Arc::new(arrow::datatypes::Schema::new(vec![id_field]));
let ids = arrow::array::Int32Array::from(vec![1, 2, 3, 4, 5]);
let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)])?;
let ops = DeltaOps::try_from_url("../path/to/empty/dir").await?;
let table = ops.write(vec![batch]).await?;
````



