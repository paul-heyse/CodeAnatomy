**datafusion_catalog_listing > table**

# Module: table

## Contents

**Structs**

- [`ListFilesResult`](#listfilesresult) - Result of a file listing operation from [`ListingTable::list_files_for_scan`].
- [`ListingTable`](#listingtable) - Built in [`TableProvider`] that reads data from one or more files as a single table.

---

## datafusion_catalog_listing::table::ListFilesResult

*Struct*

Result of a file listing operation from [`ListingTable::list_files_for_scan`].

**Fields:**
- `file_groups: Vec<datafusion_datasource::file_groups::FileGroup>` - File groups organized by the partitioning strategy.
- `statistics: datafusion_common::Statistics` - Aggregated statistics for all files.
- `grouped_by_partition: bool` - Whether files are grouped by partition values (enables Hash partitioning).

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_catalog_listing::table::ListingTable

*Struct*

Built in [`TableProvider`] that reads data from one or more files as a single table.

The files are read using an  [`ObjectStore`] instance, for example from
local files or objects from AWS S3.

# Features:
* Reading multiple files as a single table
* Hive style partitioning (e.g., directories named `date=2024-06-01`)
* Merges schemas from files with compatible but not identical schemas (see [`ListingTableConfig::file_schema`])
* `limit`, `filter` and `projection` pushdown for formats that support it (e.g.,
  Parquet)
* Statistics collection and pruning based on file metadata
* Pre-existing sort order (see [`ListingOptions::file_sort_order`])
* Metadata caching to speed up repeated queries (see [`FileMetadataCache`])
* Statistics caching (see [`FileStatisticsCache`])

[`FileMetadataCache`]: datafusion_execution::cache::cache_manager::FileMetadataCache

# Reading Directories and Hive Style Partitioning

For example, given the `table1` directory (or object store prefix)

```text
table1
 ├── file1.parquet
 └── file2.parquet
```

A `ListingTable` would read the files `file1.parquet` and `file2.parquet` as
a single table, merging the schemas if the files have compatible but not
identical schemas.

Given the `table2` directory (or object store prefix)

```text
table2
 ├── date=2024-06-01
 │    ├── file3.parquet
 │    └── file4.parquet
 └── date=2024-06-02
      └── file5.parquet
```

A `ListingTable` would read the files `file3.parquet`, `file4.parquet`, and
`file5.parquet` as a single table, again merging schemas if necessary.

Given the hive style partitioning structure (e.g,. directories named
`date=2024-06-01` and `date=2026-06-02`), `ListingTable` also adds a `date`
column when reading the table:
* The files in `table2/date=2024-06-01` will have the value `2024-06-01`
* The files in `table2/date=2024-06-02` will have the value `2024-06-02`.

If the query has a predicate like `WHERE date = '2024-06-01'`
only the corresponding directory will be read.

# See Also

1. [`ListingTableConfig`]: Configuration options
1. [`DataSourceExec`]: `ExecutionPlan` used by `ListingTable`

[`DataSourceExec`]: datafusion_datasource::source::DataSourceExec

# Caching Metadata

Some formats, such as Parquet, use the `FileMetadataCache` to cache file
metadata that is needed to execute but expensive to read, such as row
groups and statistics. The cache is scoped to the `SessionContext` and can
be configured via the [runtime config options].

[runtime config options]: https://datafusion.apache.org/user-guide/configs.html#runtime-configuration-settings

# Example: Read a directory of parquet files using a [`ListingTable`]

```no_run
# use datafusion_common::Result;
# use std::sync::Arc;
# use datafusion_catalog::TableProvider;
# use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
# use datafusion_datasource::ListingTableUrl;
# use datafusion_datasource_parquet::file_format::ParquetFormat;/// #
# use datafusion_catalog::Session;
async fn get_listing_table(session: &dyn Session) -> Result<Arc<dyn TableProvider>> {
let table_path = "/path/to/parquet";

// Parse the path
let table_path = ListingTableUrl::parse(table_path)?;

// Create default parquet options
let file_format = ParquetFormat::new();
let listing_options = ListingOptions::new(Arc::new(file_format))
  .with_file_extension(".parquet");

// Resolve the schema
let resolved_schema = listing_options
   .infer_schema(session, &table_path)
   .await?;

let config = ListingTableConfig::new(table_path)
  .with_listing_options(listing_options)
  .with_schema(resolved_schema);

// Create a new TableProvider
let provider = Arc::new(ListingTable::try_new(config)?);

# Ok(provider)
# }
```

**Methods:**

- `fn try_new(config: ListingTableConfig) -> datafusion_common::Result<Self>` - Create new [`ListingTable`]
- `fn with_constraints(self: Self, constraints: Constraints) -> Self` - Assign constraints
- `fn with_column_defaults(self: Self, column_defaults: HashMap<String, Expr>) -> Self` - Assign column defaults
- `fn with_cache(self: Self, cache: Option<Arc<dyn FileStatisticsCache>>) -> Self` - Set the [`FileStatisticsCache`] used to cache parquet file statistics.
- `fn with_definition(self: Self, definition: Option<String>) -> Self` - Specify the SQL definition for this table, if any
- `fn table_paths(self: &Self) -> &Vec<ListingTableUrl>` - Get paths ref
- `fn options(self: &Self) -> &ListingOptions` - Get options ref
- `fn schema_source(self: &Self) -> SchemaSource` - Get the schema source
- `fn with_schema_adapter_factory(self: Self, _schema_adapter_factory: Arc<dyn SchemaAdapterFactory>) -> Self` - Deprecated: Set the [`SchemaAdapterFactory`] for this [`ListingTable`]
- `fn schema_adapter_factory(self: &Self) -> Option<Arc<dyn SchemaAdapterFactory>>` - Deprecated: Returns the [`SchemaAdapterFactory`] used by this [`ListingTable`].
- `fn try_create_output_ordering(self: &Self, execution_props: &ExecutionProps) -> datafusion_common::Result<Vec<LexOrdering>>` - If file_sort_order is specified, creates the appropriate physical expressions
- `fn list_files_for_scan<'a>(self: &'a Self, ctx: &'a dyn Session, filters: &'a [Expr], limit: Option<usize>) -> datafusion_common::Result<ListFilesResult>` - Get the list of files for a scan as well as the file level statistics.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn constraints(self: &Self) -> Option<&Constraints>`
  - `fn table_type(self: &Self) -> TableType`
  - `fn scan(self: &'life0 Self, state: &'life1 dyn Session, projection: Option<&'life2 Vec<usize>>, filters: &'life3 [Expr], limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn scan_with_args<'a>(self: &'life0 Self, state: &'life1 dyn Session, args: ScanArgs<'a>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn supports_filters_pushdown(self: &Self, filters: &[&Expr]) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>>`
  - `fn get_table_definition(self: &Self) -> Option<&str>`
  - `fn insert_into(self: &'life0 Self, state: &'life1 dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn get_column_default(self: &Self, column: &str) -> Option<&Expr>`
- **Clone**
  - `fn clone(self: &Self) -> ListingTable`



