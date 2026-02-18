**datafusion_datasource > memory**

# Module: memory

## Contents

**Structs**

- [`MemSink`](#memsink) - Implements for writing to a [`MemTable`]
- [`MemorySourceConfig`](#memorysourceconfig) - Data source configuration for reading in-memory batches of data

**Type Aliases**

- [`PartitionData`](#partitiondata) - Type alias for partition data

---

## datafusion_datasource::memory::MemSink

*Struct*

Implements for writing to a [`MemTable`]

[`MemTable`]: <https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html>

**Methods:**

- `fn try_new(batches: Vec<PartitionData>, schema: SchemaRef) -> Result<Self>` - Creates a new [`MemSink`].

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **DataSink**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> &SchemaRef`
  - `fn write_all(self: &'life0 Self, data: SendableRecordBatchStream, _context: &'life1 Arc<TaskContext>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_datasource::memory::MemorySourceConfig

*Struct*

Data source configuration for reading in-memory batches of data

**Methods:**

- `fn try_new(partitions: &[Vec<RecordBatch>], schema: SchemaRef, projection: Option<Vec<usize>>) -> Result<Self>` - Create a new `MemorySourceConfig` for reading in-memory record batches
- `fn try_new_exec(partitions: &[Vec<RecordBatch>], schema: SchemaRef, projection: Option<Vec<usize>>) -> Result<Arc<DataSourceExec>>` - Create a new `DataSourceExec` plan for reading in-memory record batches
- `fn try_new_as_values(schema: SchemaRef, data: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Result<Arc<DataSourceExec>>` - Create a new execution plan from a list of constant values (`ValuesExec`)
- `fn try_new_from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Arc<DataSourceExec>>` - Create a new plan using the provided schema and batches.
- `fn with_limit(self: Self, limit: Option<usize>) -> Self` - Set the limit of the files
- `fn with_show_sizes(self: Self, show_sizes: bool) -> Self` - Set `show_sizes` to determine whether to display partition sizes
- `fn partitions(self: &Self) -> &[Vec<RecordBatch>]` - Ref to partitions
- `fn projection(self: &Self) -> &Option<Vec<usize>>` - Ref to projection
- `fn show_sizes(self: &Self) -> bool` - Show sizes
- `fn sort_information(self: &Self) -> &[LexOrdering]` - Ref to sort information
- `fn try_with_sort_information(self: Self, sort_information: Vec<LexOrdering>) -> Result<Self>` - A memory table can be ordered by multiple expressions simultaneously.
- `fn original_schema(self: &Self) -> SchemaRef` - Arc clone of ref to original schema

**Trait Implementations:**

- **DataSource**
  - `fn open(self: &Self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream>`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut fmt::Formatter) -> fmt::Result`
  - `fn repartitioned(self: &Self, target_partitions: usize, _repartition_file_min_size: usize, output_ordering: Option<LexOrdering>) -> Result<Option<Arc<dyn DataSource>>>` - If possible, redistribute batches across partitions according to their size.
  - `fn output_partitioning(self: &Self) -> Partitioning`
  - `fn eq_properties(self: &Self) -> EquivalenceProperties`
  - `fn scheduling_type(self: &Self) -> SchedulingType`
  - `fn partition_statistics(self: &Self, partition: Option<usize>) -> Result<Statistics>`
  - `fn with_fetch(self: &Self, limit: Option<usize>) -> Option<Arc<dyn DataSource>>`
  - `fn fetch(self: &Self) -> Option<usize>`
  - `fn try_swapping_with_projection(self: &Self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn DataSource>>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> MemorySourceConfig`



## datafusion_datasource::memory::PartitionData

*Type Alias*: `std::sync::Arc<tokio::sync::RwLock<Vec<arrow::array::RecordBatch>>>`

Type alias for partition data



