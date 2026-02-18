**deltalake_core > writer > record_batch**

# Module: writer::record_batch

## Contents

**Structs**

- [`PartitionResult`](#partitionresult) - Helper container for partitioned record batches
- [`RecordBatchWriter`](#recordbatchwriter) - Writes messages to a delta lake table.

---

## deltalake_core::writer::record_batch::PartitionResult

*Struct*

Helper container for partitioned record batches

**Fields:**
- `partition_values: indexmap::IndexMap<String, delta_kernel::expressions::Scalar>` - values found in partition columns
- `record_batch: arrow_array::RecordBatch` - remaining dataset with partition column values removed

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PartitionResult`



## deltalake_core::writer::record_batch::RecordBatchWriter

*Struct*

Writes messages to a delta lake table.

**Methods:**

- `fn try_new<impl AsRef<str>>(table_uri: impl Trait, schema: ArrowSchemaRef, partition_columns: Option<Vec<String>>, storage_options: Option<HashMap<String, String>>) -> Result<Self, DeltaTableError>` - Create a new [`RecordBatchWriter`] instance
- `fn with_commit_properties(self: Self, properties: CommitProperties) -> Self` - Add the [CommitProperties] to the [RecordBatchWriter] to be used when the writer flushes
- `fn for_table(table: &DeltaTable) -> Result<Self, DeltaTableError>` - Creates a [`RecordBatchWriter`] to write data to provided Delta Table
- `fn buffer_len(self: &Self) -> usize` - Returns the current byte length of the in memory buffer.
- `fn buffered_record_batch_count(self: &Self) -> usize` - Returns the number of records held in the current buffer.
- `fn reset(self: & mut Self)` - Resets internal state.
- `fn arrow_schema(self: &Self) -> ArrowSchemaRef` - Returns the arrow schema representation of the delta table schema defined for the wrapped
- `fn write_partition(self: & mut Self, record_batch: RecordBatch, partition_values: &IndexMap<String, Scalar>, mode: WriteMode) -> Result<ArrowSchemaRef, DeltaTableError>` - Write a batch to the specified partition
- `fn with_writer_properties(self: Self, writer_properties: WriterProperties) -> Self` - Sets the writer properties for the underlying arrow writer.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **DeltaWriter**
  - `fn write(self: &'life0  mut Self, values: RecordBatch) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Write a chunk of values into the internal write buffers with the default write mode
  - `fn write_with_mode(self: &'life0  mut Self, values: RecordBatch, mode: WriteMode) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Divides a single record batch into into multiple according to table partitioning.
  - `fn flush(self: &'life0  mut Self) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Writes the existing parquet bytes to storage and resets internal state to handle another file.
  - `fn flush_and_commit(self: &'life0  mut Self, table: &'life1  mut DeltaTable) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Flush the internal write buffers to files in the delta table folder structure.



