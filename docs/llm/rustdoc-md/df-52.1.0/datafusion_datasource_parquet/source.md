**datafusion_datasource_parquet > source**

# Module: source

## Contents

**Structs**

- [`ParquetSource`](#parquetsource) - Execution plan for reading one or more Parquet files.

---

## datafusion_datasource_parquet::source::ParquetSource

*Struct*

Execution plan for reading one or more Parquet files.

```text
            ▲
            │
            │  Produce a stream of
            │  RecordBatches
            │
┌───────────────────────┐
│                       │
│     DataSourceExec    │
│                       │
└───────────────────────┘
            ▲
            │  Asynchronously read from one
            │  or more parquet files via
            │  ObjectStore interface
            │
            │
  .───────────────────.
 │                     )
 │`───────────────────'│
 │    ObjectStore      │
 │.───────────────────.│
 │                     )
  `───────────────────'
```

# Example: Create a `DataSourceExec`
```
# use std::sync::Arc;
# use arrow::datatypes::Schema;
# use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
# use datafusion_datasource_parquet::source::ParquetSource;
# use datafusion_datasource::PartitionedFile;
# use datafusion_execution::object_store::ObjectStoreUrl;
# use datafusion_physical_expr::expressions::lit;
# use datafusion_datasource::source::DataSourceExec;
# use datafusion_common::config::TableParquetOptions;

# let file_schema = Arc::new(Schema::empty());
# let object_store_url = ObjectStoreUrl::local_filesystem();
# let predicate = lit(true);
let source = Arc::new(
    ParquetSource::new(Arc::clone(&file_schema))
        .with_predicate(predicate)
);
// Create a DataSourceExec for reading `file1.parquet` with a file size of 100MB
let config = FileScanConfigBuilder::new(object_store_url, source)
   .with_file(PartitionedFile::new("file1.parquet", 100*1024*1024)).build();
let exec = DataSourceExec::from_data_source(config);
```

# Features

Supports the following optimizations:

* Concurrent reads: reads from one or more files in parallel as multiple
  partitions, including concurrently reading multiple row groups from a single
  file.

* Predicate push down: skips row groups, pages, rows based on metadata
  and late materialization. See "Predicate Pushdown" below.

* Projection pushdown: reads and decodes only the columns required.

* Limit pushdown: stop execution early after some number of rows are read.

* Custom readers: customize reading  parquet files, e.g. to cache metadata,
  coalesce I/O operations, etc. See [`ParquetFileReaderFactory`] for more
  details.

* Schema evolution: read parquet files with different schemas into a unified
  table schema. See [`DefaultPhysicalExprAdapterFactory`] for more details.

* metadata_size_hint: controls the number of bytes read from the end of the
  file in the initial I/O when the default [`ParquetFileReaderFactory`]. If a
  custom reader is used, it supplies the metadata directly and this parameter
  is ignored. [`ParquetSource::with_metadata_size_hint`] for more details.

* User provided  `ParquetAccessPlan`s to skip row groups and/or pages
  based on external information. See "Implementing External Indexes" below

# Predicate Pushdown

`DataSourceExec` uses the provided [`PhysicalExpr`] predicate as a filter to
skip reading unnecessary data and improve query performance using several techniques:

* Row group pruning: skips entire row groups based on min/max statistics
  found in [`ParquetMetaData`] and any Bloom filters that are present.

* Page pruning: skips individual pages within a ColumnChunk using the
  [Parquet PageIndex], if present.

* Row filtering: skips rows within a page using a form of late
  materialization. When possible, predicates are applied by the parquet
  decoder *during* decode (see [`ArrowPredicate`] and [`RowFilter`] for more
  details). This is only enabled if `ParquetScanOptions::pushdown_filters` is set to true.

Note: If the predicate can not be used to accelerate the scan, it is ignored
(no error is raised on predicate evaluation errors).

[`ArrowPredicate`]: parquet::arrow::arrow_reader::ArrowPredicate
[`RowFilter`]: parquet::arrow::arrow_reader::RowFilter
[Parquet PageIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md

# Example: rewriting `DataSourceExec`

You can modify a `DataSourceExec` using [`ParquetSource`], for example
to change files or add a predicate.

```no_run
# use std::sync::Arc;
# use arrow::datatypes::Schema;
# use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
# use datafusion_datasource::PartitionedFile;
# use datafusion_datasource::source::DataSourceExec;

# fn parquet_exec() -> DataSourceExec { unimplemented!() }
// Split a single DataSourceExec into multiple DataSourceExecs, one for each file
let exec = parquet_exec();
let data_source = exec.data_source();
let base_config = data_source.as_any().downcast_ref::<FileScanConfig>().unwrap();
let existing_file_groups = &base_config.file_groups;
let new_execs = existing_file_groups
  .iter()
  .map(|file_group| {
    // create a new exec by copying the existing exec's source config
    let new_config = FileScanConfigBuilder::from(base_config.clone())
       .with_file_groups(vec![file_group.clone()])
      .build();

    (DataSourceExec::from_data_source(new_config))
  })
  .collect::<Vec<_>>();
```

# Implementing External Indexes

It is possible to restrict the row groups and selections within those row
groups that the DataSourceExec will consider by providing an initial
`ParquetAccessPlan` as `extensions` on `PartitionedFile`. This can be
used to implement external indexes on top of parquet files and select only
portions of the files.

The `DataSourceExec` will try and reduce any provided `ParquetAccessPlan`
further based on the contents of `ParquetMetadata` and other settings.

## Example of providing a ParquetAccessPlan

```
# use std::sync::Arc;
# use arrow::datatypes::{Schema, SchemaRef};
# use datafusion_datasource::PartitionedFile;
# use datafusion_datasource_parquet::ParquetAccessPlan;
# use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
# use datafusion_datasource_parquet::source::ParquetSource;
# use datafusion_execution::object_store::ObjectStoreUrl;
# use datafusion_datasource::source::DataSourceExec;

# fn schema() -> SchemaRef {
#   Arc::new(Schema::empty())
# }
// create an access plan to scan row group 0, 1 and 3 and skip row groups 2 and 4
let mut access_plan = ParquetAccessPlan::new_all(5);
access_plan.skip(2);
access_plan.skip(4);
// provide the plan as extension to the FileScanConfig
let partitioned_file = PartitionedFile::new("my_file.parquet", 1234)
  .with_extensions(Arc::new(access_plan));
// create a FileScanConfig to scan this file
let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(ParquetSource::new(schema())))
    .with_file(partitioned_file).build();
// this parquet DataSourceExec will not even try to read row groups 2 and 4. Additional
// pruning based on predicates may also happen
let exec = DataSourceExec::from_data_source(config);
```

For a complete example, see the [`advanced_parquet_index` example]).

[`parquet_index_advanced` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/data_io/parquet_advanced_index.rs

# Execution Overview

* Step 1: `DataSourceExec::execute` is called, returning a `FileStream`
  configured to open parquet files with a `ParquetOpener`.

* Step 2: When the stream is polled, the `ParquetOpener` is called to open
  the file.

* Step 3: The `ParquetOpener` gets the [`ParquetMetaData`] (file metadata)
  via [`ParquetFileReaderFactory`], creating a `ParquetAccessPlan` by
  applying predicates to metadata. The plan and projections are used to
  determine what pages must be read.

* Step 4: The stream begins reading data, fetching the required parquet
  pages incrementally decoding them, and applying any row filters (see
  [`Self::with_pushdown_filters`]).

* Step 5: As each [`RecordBatch`] is read, it may be adapted by a
  [`DefaultPhysicalExprAdapterFactory`] to match the table schema. By default missing columns are
  filled with nulls, but this can be customized via [`PhysicalExprAdapterFactory`].

[`RecordBatch`]: arrow::record_batch::RecordBatch
[`ParquetMetadata`]: parquet::file::metadata::ParquetMetaData
[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory

**Methods:**

- `fn new<impl Into<TableSchema>>(table_schema: impl Trait) -> Self` - Create a new ParquetSource to read the data specified in the file scan
- `fn with_table_parquet_options(self: Self, table_parquet_options: TableParquetOptions) -> Self` - Set the `TableParquetOptions` for this ParquetSource.
- `fn with_metadata_size_hint(self: Self, metadata_size_hint: usize) -> Self` - Set the metadata size hint
- `fn with_predicate(self: &Self, predicate: Arc<dyn PhysicalExpr>) -> Self` - Set predicate information
- `fn table_parquet_options(self: &Self) -> &TableParquetOptions` - Options passed to the parquet reader for this scan
- `fn predicate(self: &Self) -> Option<&Arc<dyn PhysicalExpr>>` - Optional predicate.
- `fn parquet_file_reader_factory(self: &Self) -> Option<&Arc<dyn ParquetFileReaderFactory>>` - return the optional file reader factory
- `fn with_parquet_file_reader_factory(self: Self, parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>) -> Self` - Optional user defined parquet file reader factory.
- `fn with_pushdown_filters(self: Self, pushdown_filters: bool) -> Self` - If true, the predicate will be used during the parquet scan.
- `fn with_reorder_filters(self: Self, reorder_filters: bool) -> Self` - If true, the `RowFilter` made by `pushdown_filters` may try to
- `fn with_enable_page_index(self: Self, enable_page_index: bool) -> Self` - If enabled, the reader will read the page index
- `fn with_bloom_filter_on_read(self: Self, bloom_filter_on_read: bool) -> Self` - If enabled, the reader will read by the bloom filter
- `fn with_bloom_filter_on_write(self: Self, enable_bloom_filter_on_write: bool) -> Self` - If enabled, the writer will write by the bloom filter
- `fn max_predicate_cache_size(self: &Self) -> Option<usize>` - Return the maximum predicate cache size, in bytes, used when

**Trait Implementations:**

- **FileSource**
  - `fn create_file_opener(self: &Self, object_store: Arc<dyn ObjectStore>, base_config: &FileScanConfig, partition: usize) -> datafusion_common::Result<Arc<dyn FileOpener>>`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn table_schema(self: &Self) -> &TableSchema`
  - `fn filter(self: &Self) -> Option<Arc<dyn PhysicalExpr>>`
  - `fn with_batch_size(self: &Self, batch_size: usize) -> Arc<dyn FileSource>`
  - `fn try_pushdown_projection(self: &Self, projection: &ProjectionExprs) -> datafusion_common::Result<Option<Arc<dyn FileSource>>>`
  - `fn projection(self: &Self) -> Option<&ProjectionExprs>`
  - `fn metrics(self: &Self) -> &ExecutionPlanMetricsSet`
  - `fn file_type(self: &Self) -> &str`
  - `fn fmt_extra(self: &Self, t: DisplayFormatType, f: & mut Formatter) -> std::fmt::Result`
  - `fn try_pushdown_filters(self: &Self, filters: Vec<Arc<dyn PhysicalExpr>>, config: &ConfigOptions) -> datafusion_common::Result<FilterPushdownPropagation<Arc<dyn FileSource>>>`
  - `fn try_reverse_output(self: &Self, order: &[PhysicalSortExpr], eq_properties: &EquivalenceProperties) -> datafusion_common::Result<SortOrderPushdownResult<Arc<dyn FileSource>>>` - Try to optimize the scan to produce data in the requested sort order.
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ParquetSource`



