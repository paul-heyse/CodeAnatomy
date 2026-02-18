**datafusion_datasource > write**

# Module: write

## Contents

**Modules**

- [`demux`](#demux) - Module containing helper methods/traits related to enabling
- [`orchestration`](#orchestration) - Module containing helper methods/traits related to

**Structs**

- [`ObjectWriterBuilder`](#objectwriterbuilder) - A builder for an [`AsyncWrite`] that writes to an object store location.
- [`SharedBuffer`](#sharedbuffer) - A buffer with interior mutability shared by the SerializedFileWriter and

**Functions**

- [`create_writer`](#create_writer) - Returns an [`AsyncWrite`] which writes to the given object store location
- [`get_writer_schema`](#get_writer_schema) - Converts table schema to writer schema, which may differ in the case

**Traits**

- [`BatchSerializer`](#batchserializer) - A trait that defines the methods required for a RecordBatch serializer.

---

## datafusion_datasource::write::BatchSerializer

*Trait*

A trait that defines the methods required for a RecordBatch serializer.

**Methods:**

- `serialize`: Asynchronously serializes a `RecordBatch` and returns the serialized bytes.



## datafusion_datasource::write::ObjectWriterBuilder

*Struct*

A builder for an [`AsyncWrite`] that writes to an object store location.

This can be used to specify file compression on the writer. The writer
will have a default buffer size unless altered. The specific default size
is chosen by [`BufWriter::new`].

We drop the `AbortableWrite` struct and the writer will not try to cleanup on failure.
Users can configure automatic cleanup with their cloud provider.

**Methods:**

- `fn new(file_compression_type: FileCompressionType, location: &Path, object_store: Arc<dyn ObjectStore>) -> Self` - Create a new [`ObjectWriterBuilder`] for the specified path and compression type.
- `fn set_buffer_size(self: & mut Self, buffer_size: Option<usize>)` - Set buffer size in bytes for object writer.
- `fn with_buffer_size(self: Self, buffer_size: Option<usize>) -> Self` - Set buffer size in bytes for object writer, returning the builder.
- `fn get_buffer_size(self: &Self) -> Option<usize>` - Currently specified buffer size in bytes.
- `fn set_compression_level(self: & mut Self, compression_level: Option<u32>)` - Set compression level for object writer.
- `fn with_compression_level(self: Self, compression_level: Option<u32>) -> Self` - Set compression level for object writer, returning the builder.
- `fn get_compression_level(self: &Self) -> Option<u32>` - Currently specified compression level.
- `fn build(self: Self) -> Result<Box<dyn AsyncWrite>>` - Return a writer object that writes to the object store location.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource::write::SharedBuffer

*Struct*

A buffer with interior mutability shared by the SerializedFileWriter and
ObjectStore writer

**Fields:**
- `buffer: std::sync::Arc<futures::lock::Mutex<Vec<u8>>>` - The inner buffer for reading and writing

**Methods:**

- `fn new(capacity: usize) -> Self`

**Trait Implementations:**

- **Write**
  - `fn write(self: & mut Self, buf: &[u8]) -> std::io::Result<usize>`
  - `fn flush(self: & mut Self) -> std::io::Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> SharedBuffer`



## datafusion_datasource::write::create_writer

*Function*

Returns an [`AsyncWrite`] which writes to the given object store location
with the specified compression.

The writer will have a default buffer size as chosen by [`BufWriter::new`].

We drop the `AbortableWrite` struct and the writer will not try to cleanup on failure.
Users can configure automatic cleanup with their cloud provider.

```rust
fn create_writer(file_compression_type: crate::file_compression_type::FileCompressionType, location: &object_store::path::Path, object_store: std::sync::Arc<dyn ObjectStore>) -> datafusion_common::error::Result<Box<dyn AsyncWrite>>
```



## Module: demux

Module containing helper methods/traits related to enabling
dividing input stream into multiple output files at execution time



## datafusion_datasource::write::get_writer_schema

*Function*

Converts table schema to writer schema, which may differ in the case
of hive style partitioning where some columns are removed from the
underlying files.

```rust
fn get_writer_schema(config: &crate::file_sink_config::FileSinkConfig) -> std::sync::Arc<arrow::datatypes::Schema>
```



## Module: orchestration

Module containing helper methods/traits related to
orchestrating file serialization, streaming to object store,
parallelization, and abort handling



