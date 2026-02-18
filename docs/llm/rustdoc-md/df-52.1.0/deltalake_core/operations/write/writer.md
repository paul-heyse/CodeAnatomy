**deltalake_core > operations > write > writer**

# Module: operations::write::writer

## Contents

**Structs**

- [`DeltaWriter`](#deltawriter) - A parquet writer implementation tailored to the needs of writing data to a delta table.
- [`PartitionWriter`](#partitionwriter) - Partition writer implementation
- [`PartitionWriterConfig`](#partitionwriterconfig) - Write configuration for partition writers
- [`WriterConfig`](#writerconfig) - Configuration to write data into Delta tables

---

## deltalake_core::operations::write::writer::DeltaWriter

*Struct*

A parquet writer implementation tailored to the needs of writing data to a delta table.

**Methods:**

- `fn new(object_store: ObjectStoreRef, config: WriterConfig) -> Self` - Create a new instance of [`DeltaWriter`]
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Apply custom writer_properties to the underlying parquet writer
- `fn write_partition(self: & mut Self, record_batch: RecordBatch, partition_values: &IndexMap<String, Scalar>) -> DeltaResult<()>` - Write a batch to the partition induced by the partition_values. The record batch is expected
- `fn write(self: & mut Self, batch: &RecordBatch) -> DeltaResult<()>` - Buffers record batches in-memory per partition up to appx. `target_file_size` for a partition.
- `fn close(self: Self) -> DeltaResult<Vec<Add>>` - Close the writer and get the new [Add] actions.



## deltalake_core::operations::write::writer::PartitionWriter

*Struct*

Partition writer implementation
This writer takes in table data as RecordBatches and writes it out to partitioned parquet files.
It buffers data in memory until it reaches a certain size, then writes it out to optimize file sizes.
When you complete writing you get back a list of Add actions that can be used to update the Delta table commit log.

**Methods:**

- `fn try_with_config(object_store: ObjectStoreRef, config: PartitionWriterConfig, num_indexed_cols: DataSkippingNumIndexedCols, stats_columns: Option<Vec<String>>) -> DeltaResult<Self>` - Create a new instance of [`PartitionWriter`] from [`PartitionWriterConfig`]
- `fn write(self: & mut Self, batch: &RecordBatch) -> DeltaResult<()>` - Buffers record batches in-memory up to appx. `target_file_size`.
- `fn close(self: Self) -> DeltaResult<Vec<Add>>` - Close the writer and get the new [Add] actions.



## deltalake_core::operations::write::writer::PartitionWriterConfig

*Struct*

Write configuration for partition writers

**Methods:**

- `fn try_new(file_schema: ArrowSchemaRef, partition_values: IndexMap<String, Scalar>, writer_properties: Option<WriterProperties>, target_file_size: Option<usize>, write_batch_size: Option<usize>, max_concurrency_tasks: Option<usize>) -> DeltaResult<Self>` - Create a new instance of [PartitionWriterConfig]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PartitionWriterConfig`



## deltalake_core::operations::write::writer::WriterConfig

*Struct*

Configuration to write data into Delta tables

**Methods:**

- `fn new(table_schema: ArrowSchemaRef, partition_columns: Vec<String>, writer_properties: Option<WriterProperties>, target_file_size: Option<usize>, write_batch_size: Option<usize>, num_indexed_cols: DataSkippingNumIndexedCols, stats_columns: Option<Vec<String>>) -> Self` - Create a new instance of [WriterConfig].
- `fn file_schema(self: &Self) -> ArrowSchemaRef` - Schema of files written to disk

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> WriterConfig`



