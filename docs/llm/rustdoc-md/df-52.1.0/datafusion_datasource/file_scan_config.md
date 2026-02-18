**datafusion_datasource > file_scan_config**

# Module: file_scan_config

## Contents

**Structs**

- [`FileScanConfig`](#filescanconfig) - The base configurations for a [`DataSourceExec`], the a physical plan for
- [`FileScanConfigBuilder`](#filescanconfigbuilder) - A builder for [`FileScanConfig`]'s.

**Functions**

- [`wrap_partition_type_in_dict`](#wrap_partition_type_in_dict) - Convert type to a type suitable for use as a `ListingTable`
- [`wrap_partition_value_in_dict`](#wrap_partition_value_in_dict) - Convert a [`ScalarValue`] of partition columns to a type, as

---

## datafusion_datasource::file_scan_config::FileScanConfig

*Struct*

The base configurations for a [`DataSourceExec`], the a physical plan for
any given file format.

Use [`DataSourceExec::from_data_source`] to create a [`DataSourceExec`] from a ``FileScanConfig`.

# Example
```
# use std::any::Any;
# use std::sync::Arc;
# use arrow::datatypes::{Field, Fields, DataType, Schema, SchemaRef};
# use object_store::ObjectStore;
# use datafusion_common::Result;
# use datafusion_datasource::file::FileSource;
# use datafusion_datasource::file_groups::FileGroup;
# use datafusion_datasource::PartitionedFile;
# use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
# use datafusion_datasource::file_stream::FileOpener;
# use datafusion_datasource::source::DataSourceExec;
# use datafusion_datasource::table_schema::TableSchema;
# use datafusion_execution::object_store::ObjectStoreUrl;
# use datafusion_physical_expr::projection::ProjectionExprs;
# use datafusion_physical_plan::ExecutionPlan;
# use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
# let file_schema = Arc::new(Schema::new(vec![
#  Field::new("c1", DataType::Int32, false),
#  Field::new("c2", DataType::Int32, false),
#  Field::new("c3", DataType::Int32, false),
#  Field::new("c4", DataType::Int32, false),
# ]));
# // Note: crate mock ParquetSource, as ParquetSource is not in the datasource crate
#[derive(Clone)]
# struct ParquetSource {
#    table_schema: TableSchema,
# };
# impl FileSource for ParquetSource {
#  fn create_file_opener(&self, _: Arc<dyn ObjectStore>, _: &FileScanConfig, _: usize) -> Result<Arc<dyn FileOpener>> { unimplemented!() }
#  fn as_any(&self) -> &dyn Any { self  }
#  fn table_schema(&self) -> &TableSchema { &self.table_schema }
#  fn with_batch_size(&self, _: usize) -> Arc<dyn FileSource> { unimplemented!() }
#  fn metrics(&self) -> &ExecutionPlanMetricsSet { unimplemented!() }
#  fn file_type(&self) -> &str { "parquet" }
#  // Note that this implementation drops the projection on the floor, it is not complete!
#  fn try_pushdown_projection(&self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn FileSource>>> { Ok(Some(Arc::new(self.clone()) as Arc<dyn FileSource>)) }
#  }
# impl ParquetSource {
#  fn new(table_schema: impl Into<TableSchema>) -> Self { Self {table_schema: table_schema.into()} }
# }
// create FileScan config for reading parquet files from file://
let object_store_url = ObjectStoreUrl::local_filesystem();
let file_source = Arc::new(ParquetSource::new(file_schema.clone()));
let config = FileScanConfigBuilder::new(object_store_url, file_source)
  .with_limit(Some(1000))            // read only the first 1000 records
  .with_projection_indices(Some(vec![2, 3])) // project columns 2 and 3
  .expect("Failed to push down projection")
   // Read /tmp/file1.parquet with known size of 1234 bytes in a single group
  .with_file(PartitionedFile::new("file1.parquet", 1234))
  // Read /tmp/file2.parquet 56 bytes and /tmp/file3.parquet 78 bytes
  // in a  single row group
  .with_file_group(FileGroup::new(vec![
   PartitionedFile::new("file2.parquet", 56),
   PartitionedFile::new("file3.parquet", 78),
  ])).build();
// create an execution plan from the config
let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
```

[`DataSourceExec`]: crate::source::DataSourceExec
[`DataSourceExec::from_data_source`]: crate::source::DataSourceExec::from_data_source

**Fields:**
- `object_store_url: datafusion_execution::object_store::ObjectStoreUrl` - Object store URL, used to get an [`ObjectStore`] instance from
- `file_groups: Vec<crate::file_groups::FileGroup>` - List of files to be processed, grouped into partitions
- `constraints: datafusion_common::Constraints` - Table constraints
- `limit: Option<usize>` - The maximum number of records to read from this plan. If `None`,
- `output_ordering: Vec<datafusion_physical_expr_common::sort_expr::LexOrdering>` - All equivalent lexicographical orderings that describe the schema.
- `file_compression_type: crate::file_compression_type::FileCompressionType` - File compression type
- `file_source: std::sync::Arc<dyn FileSource>` - File source such as `ParquetSource`, `CsvSource`, `JsonSource`, etc.
- `batch_size: Option<usize>` - Batch size while creating new batches
- `expr_adapter_factory: Option<std::sync::Arc<dyn PhysicalExprAdapterFactory>>` - Expression adapter used to adapt filters and projections that are pushed down into the scan
- `partitioned_by_file_group: bool` - When true, file_groups are organized by partition column values

**Methods:**

- `fn file_schema(self: &Self) -> &SchemaRef` - Get the file schema (schema of the files without partition columns)
- `fn table_partition_cols(self: &Self) -> &Vec<FieldRef>` - Get the table partition columns
- `fn statistics(self: &Self) -> Statistics` - Returns the unprojected table statistics, marking them as inexact if filters are present.
- `fn projected_schema(self: &Self) -> Result<Arc<Schema>>`
- `fn newlines_in_values(self: &Self) -> bool` - Returns whether newlines in values are supported.
- `fn projected_constraints(self: &Self) -> Constraints`
- `fn file_column_projection_indices(self: &Self) -> Option<Vec<usize>>`
- `fn split_groups_by_statistics_with_target_partitions(table_schema: &SchemaRef, file_groups: &[FileGroup], sort_order: &LexOrdering, target_partitions: usize) -> Result<Vec<FileGroup>>` - Splits file groups into new groups based on statistics to enable efficient parallel processing.
- `fn split_groups_by_statistics(table_schema: &SchemaRef, file_groups: &[FileGroup], sort_order: &LexOrdering) -> Result<Vec<FileGroup>>` - Attempts to do a bin-packing on files into file groups, such that any two files
- `fn file_source(self: &Self) -> &Arc<dyn FileSource>` - Returns the file_source

**Trait Implementations:**

- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut Formatter) -> FmtResult`
- **DataSource**
  - `fn open(self: &Self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut Formatter) -> FmtResult`
  - `fn repartitioned(self: &Self, target_partitions: usize, repartition_file_min_size: usize, output_ordering: Option<LexOrdering>) -> Result<Option<Arc<dyn DataSource>>>` - If supported by the underlying [`FileSource`], redistribute files across partitions according to their size.
  - `fn output_partitioning(self: &Self) -> Partitioning` - Returns the output partitioning for this file scan.
  - `fn eq_properties(self: &Self) -> EquivalenceProperties`
  - `fn scheduling_type(self: &Self) -> SchedulingType`
  - `fn partition_statistics(self: &Self, partition: Option<usize>) -> Result<Statistics>`
  - `fn with_fetch(self: &Self, limit: Option<usize>) -> Option<Arc<dyn DataSource>>`
  - `fn fetch(self: &Self) -> Option<usize>`
  - `fn metrics(self: &Self) -> ExecutionPlanMetricsSet`
  - `fn try_swapping_with_projection(self: &Self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn DataSource>>>`
  - `fn try_pushdown_filters(self: &Self, filters: Vec<Arc<dyn PhysicalExpr>>, config: &ConfigOptions) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>>`
  - `fn try_pushdown_sort(self: &Self, order: &[PhysicalSortExpr]) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> FmtResult`
- **Clone**
  - `fn clone(self: &Self) -> FileScanConfig`



## datafusion_datasource::file_scan_config::FileScanConfigBuilder

*Struct*

A builder for [`FileScanConfig`]'s.

Example:

```rust
# use std::sync::Arc;
# use arrow::datatypes::{DataType, Field, Schema};
# use datafusion_datasource::file_scan_config::{FileScanConfigBuilder, FileScanConfig};
# use datafusion_datasource::file_compression_type::FileCompressionType;
# use datafusion_datasource::file_groups::FileGroup;
# use datafusion_datasource::PartitionedFile;
# use datafusion_datasource::table_schema::TableSchema;
# use datafusion_execution::object_store::ObjectStoreUrl;
# use datafusion_common::Statistics;
# use datafusion_datasource::file::FileSource;

# fn main() {
# fn with_source(file_source: Arc<dyn FileSource>) {
    // Create a schema for our Parquet files
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Create partition columns
    let partition_cols = vec![
        Arc::new(Field::new("date", DataType::Utf8, false)),
    ];

    // Create table schema with file schema and partition columns
    let table_schema = TableSchema::new(file_schema, partition_cols);

    // Create a builder for scanning Parquet files from a local filesystem
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        file_source,
    )
    // Set a limit of 1000 rows
    .with_limit(Some(1000))
    // Project only the first column
    .with_projection_indices(Some(vec![0]))
    .expect("Failed to push down projection")
    // Add a file group with two files
    .with_file_group(FileGroup::new(vec![
        PartitionedFile::new("data/date=2024-01-01/file1.parquet", 1024),
        PartitionedFile::new("data/date=2024-01-01/file2.parquet", 2048),
    ]))
    // Set compression type
    .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
    // Build the final config
    .build();
# }
# }
```

**Methods:**

- `fn new(object_store_url: ObjectStoreUrl, file_source: Arc<dyn FileSource>) -> Self` - Create a new [`FileScanConfigBuilder`] with default settings for scanning files.
- `fn with_limit(self: Self, limit: Option<usize>) -> Self` - Set the maximum number of records to read from this plan. If `None`,
- `fn with_source(self: Self, file_source: Arc<dyn FileSource>) -> Self` - Set the file source for scanning files.
- `fn table_schema(self: &Self) -> &SchemaRef`
- `fn with_projection(self: Self, indices: Option<Vec<usize>>) -> Self` - Set the columns on which to project the data. Indexes that are higher than the
- `fn with_projection_indices(self: Self, indices: Option<Vec<usize>>) -> Result<Self>` - Set the columns on which to project the data using column indices.
- `fn with_constraints(self: Self, constraints: Constraints) -> Self` - Set the table constraints
- `fn with_statistics(self: Self, statistics: Statistics) -> Self` - Set the estimated overall statistics of the files, taking `filters` into account.
- `fn with_file_groups(self: Self, file_groups: Vec<FileGroup>) -> Self` - Set the list of files to be processed, grouped into partitions.
- `fn with_file_group(self: Self, file_group: FileGroup) -> Self` - Add a new file group
- `fn with_file(self: Self, partitioned_file: PartitionedFile) -> Self` - Add a file as a single group
- `fn with_output_ordering(self: Self, output_ordering: Vec<LexOrdering>) -> Self` - Set the output ordering of the files
- `fn with_file_compression_type(self: Self, file_compression_type: FileCompressionType) -> Self` - Set the file compression type
- `fn with_batch_size(self: Self, batch_size: Option<usize>) -> Self` - Set the batch_size property
- `fn with_expr_adapter(self: Self, expr_adapter: Option<Arc<dyn PhysicalExprAdapterFactory>>) -> Self` - Register an expression adapter used to adapt filters and projections that are pushed down into the scan
- `fn with_partitioned_by_file_group(self: Self, partitioned_by_file_group: bool) -> Self` - Set whether file groups are organized by partition column values.
- `fn build(self: Self) -> FileScanConfig` - Build the final [`FileScanConfig`] with all the configured settings.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> FileScanConfigBuilder`
- **From**
  - `fn from(config: FileScanConfig) -> Self`



## datafusion_datasource::file_scan_config::wrap_partition_type_in_dict

*Function*

Convert type to a type suitable for use as a `ListingTable`
partition column. Returns `Dictionary(UInt16, val_type)`, which is
a reasonable trade off between a reasonable number of partition
values and space efficiency.

This use this to specify types for partition columns. However
you MAY also choose not to dictionary-encode the data or to use a
different dictionary type.

Use [`wrap_partition_value_in_dict`] to wrap a [`ScalarValue`] in the same say.

```rust
fn wrap_partition_type_in_dict(val_type: arrow::datatypes::DataType) -> arrow::datatypes::DataType
```



## datafusion_datasource::file_scan_config::wrap_partition_value_in_dict

*Function*

Convert a [`ScalarValue`] of partition columns to a type, as
described in the documentation of [`wrap_partition_type_in_dict`],
which can wrap the types.

```rust
fn wrap_partition_value_in_dict(val: datafusion_common::ScalarValue) -> datafusion_common::ScalarValue
```



