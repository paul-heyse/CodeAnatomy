**datafusion_datasource_parquet > file_format**

# Module: file_format

## Contents

**Structs**

- [`ObjectStoreFetch`](#objectstorefetch) - [`MetadataFetch`] adapter for reading bytes from an [`ObjectStore`]
- [`ParquetFormat`](#parquetformat) - The Apache Parquet `FileFormat` implementation
- [`ParquetFormatFactory`](#parquetformatfactory) - Factory struct used to create [ParquetFormat]
- [`ParquetSink`](#parquetsink) - Implements [`DataSink`] for writing to a parquet file.

**Functions**

- [`apply_file_schema_type_coercions`](#apply_file_schema_type_coercions) - Apply necessary schema type coercions to make file schema match table schema.
- [`coerce_file_schema_to_string_type`](#coerce_file_schema_to_string_type) - If the table schema uses a string type, coerce the file schema to use a string type.
- [`coerce_file_schema_to_view_type`](#coerce_file_schema_to_view_type) - Coerces the file schema if the table schema uses a view type.
- [`coerce_int96_to_resolution`](#coerce_int96_to_resolution) - Coerces the file schema's Timestamps to the provided TimeUnit if Parquet schema contains INT96.
- [`fetch_parquet_metadata`](#fetch_parquet_metadata) - Fetches parquet metadata from ObjectStore for given object
- [`fetch_statistics`](#fetch_statistics) - Read and parse the statistics of the Parquet file at location `path`
- [`statistics_from_parquet_meta_calc`](#statistics_from_parquet_meta_calc)
- [`transform_binary_to_string`](#transform_binary_to_string) - Transform a schema so that any binary types are strings
- [`transform_schema_to_view`](#transform_schema_to_view) - Transform a schema to use view types for Utf8 and Binary

---

## datafusion_datasource_parquet::file_format::ObjectStoreFetch

*Struct*

[`MetadataFetch`] adapter for reading bytes from an [`ObjectStore`]

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(store: &'a dyn ObjectStore, meta: &'a ObjectMeta) -> Self`

**Trait Implementations:**

- **MetadataFetch**
  - `fn fetch(self: & mut Self, range: Range<u64>) -> BoxFuture<Result<Bytes, ParquetError>>`



## datafusion_datasource_parquet::file_format::ParquetFormat

*Struct*

The Apache Parquet `FileFormat` implementation

**Methods:**

- `fn new() -> Self` - Construct a new Format with no local overrides
- `fn with_enable_pruning(self: Self, enable: bool) -> Self` - Activate statistics based row group level pruning
- `fn enable_pruning(self: &Self) -> bool` - Return `true` if pruning is enabled
- `fn with_metadata_size_hint(self: Self, size_hint: Option<usize>) -> Self` - Provide a hint to the size of the file metadata. If a hint is provided
- `fn metadata_size_hint(self: &Self) -> Option<usize>` - Return the metadata size hint if set
- `fn with_skip_metadata(self: Self, skip_metadata: bool) -> Self` - Tell the parquet reader to skip any metadata that may be in
- `fn skip_metadata(self: &Self) -> bool` - Returns `true` if schema metadata will be cleared prior to
- `fn with_options(self: Self, options: TableParquetOptions) -> Self` - Set Parquet options for the ParquetFormat
- `fn options(self: &Self) -> &TableParquetOptions` - Parquet options
- `fn force_view_types(self: &Self) -> bool` - Return `true` if should use view types.
- `fn with_force_view_types(self: Self, use_views: bool) -> Self` - If true, will use view types. See [`Self::force_view_types`] for details
- `fn binary_as_string(self: &Self) -> bool` - Return `true` if binary types will be read as strings.
- `fn with_binary_as_string(self: Self, binary_as_string: bool) -> Self` - If true, will read binary types as strings. See [`Self::binary_as_string`] for details
- `fn coerce_int96(self: &Self) -> Option<String>`
- `fn with_coerce_int96(self: Self, time_unit: Option<String>) -> Self`

**Trait Implementations:**

- **Default**
  - `fn default() -> ParquetFormat`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FileFormat**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn get_ext(self: &Self) -> String`
  - `fn get_ext_with_compression(self: &Self, file_compression_type: &FileCompressionType) -> Result<String>`
  - `fn compression_type(self: &Self) -> Option<FileCompressionType>`
  - `fn infer_schema(self: &'life0 Self, state: &'life1 dyn Session, store: &'life2 Arc<dyn ObjectStore>, objects: &'life3 [ObjectMeta]) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn infer_stats(self: &'life0 Self, state: &'life1 dyn Session, store: &'life2 Arc<dyn ObjectStore>, table_schema: SchemaRef, object: &'life3 ObjectMeta) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn create_physical_plan(self: &'life0 Self, state: &'life1 dyn Session, conf: FileScanConfig) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn create_writer_physical_plan(self: &'life0 Self, input: Arc<dyn ExecutionPlan>, _state: &'life1 dyn Session, conf: FileSinkConfig, order_requirements: Option<LexRequirement>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn file_source(self: &Self, table_schema: TableSchema) -> Arc<dyn FileSource>`



## datafusion_datasource_parquet::file_format::ParquetFormatFactory

*Struct*

Factory struct used to create [ParquetFormat]

**Fields:**
- `options: Option<datafusion_common::config::TableParquetOptions>` - inner options for parquet

**Methods:**

- `fn new() -> Self` - Creates an instance of [ParquetFormatFactory]
- `fn new_with_options(options: TableParquetOptions) -> Self` - Creates an instance of [ParquetFormatFactory] with customized default options

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **FileFormatFactory**
  - `fn create(self: &Self, state: &dyn Session, format_options: &std::collections::HashMap<String, String>) -> Result<Arc<dyn FileFormat>>`
  - `fn default(self: &Self) -> Arc<dyn FileFormat>`
  - `fn as_any(self: &Self) -> &dyn Any`
- **Default**
  - `fn default() -> ParquetFormatFactory`
- **GetExt**
  - `fn get_ext(self: &Self) -> String`



## datafusion_datasource_parquet::file_format::ParquetSink

*Struct*

Implements [`DataSink`] for writing to a parquet file.

**Methods:**

- `fn new(config: FileSinkConfig, parquet_options: TableParquetOptions) -> Self` - Create from config.
- `fn written(self: &Self) -> HashMap<Path, ParquetMetaData>` - Retrieve the file metadata for the written files, keyed to the path
- `fn parquet_options(self: &Self) -> &TableParquetOptions` - Parquet options

**Trait Implementations:**

- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut fmt::Formatter) -> fmt::Result`
- **DataSink**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> &SchemaRef`
  - `fn write_all(self: &'life0 Self, data: SendableRecordBatchStream, context: &'life1 Arc<TaskContext>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **FileSink**
  - `fn config(self: &Self) -> &FileSinkConfig`
  - `fn spawn_writer_tasks_and_join(self: &'life0 Self, context: &'life1 Arc<TaskContext>, demux_task: SpawnedTask<Result<()>>, file_stream_rx: DemuxedStreamReceiver, object_store: Arc<dyn ObjectStore>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_datasource_parquet::file_format::apply_file_schema_type_coercions

*Function*

Apply necessary schema type coercions to make file schema match table schema.

This function performs two main types of transformations in a single pass:
1. Binary types to string types conversion - Converts binary data types to their
   corresponding string types when the table schema expects string data
2. Regular to view types conversion - Converts standard string/binary types to
   view types when the table schema uses view types

# Arguments
* `table_schema` - The table schema containing the desired types
* `file_schema` - The file schema to be transformed

# Returns
* `Some(Schema)` - If any transformations were applied, returns the transformed schema
* `None` - If no transformations were needed

```rust
fn apply_file_schema_type_coercions(table_schema: &arrow::datatypes::Schema, file_schema: &arrow::datatypes::Schema) -> Option<arrow::datatypes::Schema>
```



## datafusion_datasource_parquet::file_format::coerce_file_schema_to_string_type

*Function*

If the table schema uses a string type, coerce the file schema to use a string type.

See [ParquetFormat::binary_as_string] for details

```rust
fn coerce_file_schema_to_string_type(table_schema: &arrow::datatypes::Schema, file_schema: &arrow::datatypes::Schema) -> Option<arrow::datatypes::Schema>
```



## datafusion_datasource_parquet::file_format::coerce_file_schema_to_view_type

*Function*

Coerces the file schema if the table schema uses a view type.

```rust
fn coerce_file_schema_to_view_type(table_schema: &arrow::datatypes::Schema, file_schema: &arrow::datatypes::Schema) -> Option<arrow::datatypes::Schema>
```



## datafusion_datasource_parquet::file_format::coerce_int96_to_resolution

*Function*

Coerces the file schema's Timestamps to the provided TimeUnit if Parquet schema contains INT96.

```rust
fn coerce_int96_to_resolution(parquet_schema: &parquet::schema::types::SchemaDescriptor, file_schema: &arrow::datatypes::Schema, time_unit: &arrow::datatypes::TimeUnit) -> Option<arrow::datatypes::Schema>
```



## datafusion_datasource_parquet::file_format::fetch_parquet_metadata

*Function*

Fetches parquet metadata from ObjectStore for given object

This component is a subject to **change** in near future and is exposed for low level integrations
through [`ParquetFileReaderFactory`].

[`ParquetFileReaderFactory`]: crate::ParquetFileReaderFactory

```rust
fn fetch_parquet_metadata(store: &dyn ObjectStore, object_meta: &object_store::ObjectMeta, size_hint: Option<usize>, decryption_properties: Option<&datafusion_common::encryption::FileDecryptionProperties>, file_metadata_cache: Option<std::sync::Arc<dyn FileMetadataCache>>) -> datafusion_common::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>
```



## datafusion_datasource_parquet::file_format::fetch_statistics

*Function*

Read and parse the statistics of the Parquet file at location `path`

See [`statistics_from_parquet_meta_calc`] for more details

```rust
fn fetch_statistics(store: &dyn ObjectStore, table_schema: arrow::datatypes::SchemaRef, file: &object_store::ObjectMeta, metadata_size_hint: Option<usize>, decryption_properties: Option<&datafusion_common::encryption::FileDecryptionProperties>, file_metadata_cache: Option<std::sync::Arc<dyn FileMetadataCache>>) -> datafusion_common::Result<datafusion_common::Statistics>
```



## datafusion_datasource_parquet::file_format::statistics_from_parquet_meta_calc

*Function*

```rust
fn statistics_from_parquet_meta_calc(metadata: &parquet::file::metadata::ParquetMetaData, table_schema: arrow::datatypes::SchemaRef) -> datafusion_common::Result<datafusion_common::Statistics>
```



## datafusion_datasource_parquet::file_format::transform_binary_to_string

*Function*

Transform a schema so that any binary types are strings

```rust
fn transform_binary_to_string(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema
```



## datafusion_datasource_parquet::file_format::transform_schema_to_view

*Function*

Transform a schema to use view types for Utf8 and Binary

See [ParquetFormat::force_view_types] for details

```rust
fn transform_schema_to_view(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema
```



