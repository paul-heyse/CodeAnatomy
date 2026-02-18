**datafusion_datasource_parquet > reader**

# Module: reader

## Contents

**Structs**

- [`CachedParquetFileReader`](#cachedparquetfilereader) - Implements [`AsyncFileReader`] for a Parquet file in object storage. Reads the file metadata
- [`CachedParquetFileReaderFactory`](#cachedparquetfilereaderfactory) - Implementation of [`ParquetFileReaderFactory`] supporting the caching of footer and page
- [`CachedParquetMetaData`](#cachedparquetmetadata) - Wrapper to implement [`FileMetadata`] for [`ParquetMetaData`].
- [`DefaultParquetFileReaderFactory`](#defaultparquetfilereaderfactory) - Default implementation of [`ParquetFileReaderFactory`]
- [`ParquetFileReader`](#parquetfilereader) - Implements [`AsyncFileReader`] for a parquet file in object storage.

**Traits**

- [`ParquetFileReaderFactory`](#parquetfilereaderfactory) - Interface for reading parquet files.

---

## datafusion_datasource_parquet::reader::CachedParquetFileReader

*Struct*

Implements [`AsyncFileReader`] for a Parquet file in object storage. Reads the file metadata
from the [`FileMetadataCache`], if available, otherwise reads it directly from the file and then
updates the cache.

**Fields:**
- `file_metrics: crate::ParquetFileMetrics`
- `inner: parquet::arrow::async_reader::ParquetObjectReader`

**Methods:**

- `fn new(file_metrics: ParquetFileMetrics, store: Arc<dyn ObjectStore>, inner: ParquetObjectReader, partitioned_file: PartitionedFile, metadata_cache: Arc<dyn FileMetadataCache>, metadata_size_hint: Option<usize>) -> Self`

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **AsyncFileReader**
  - `fn get_bytes(self: & mut Self, range: Range<u64>) -> BoxFuture<parquet::errors::Result<Bytes>>`
  - `fn get_byte_ranges(self: & mut Self, ranges: Vec<Range<u64>>) -> BoxFuture<parquet::errors::Result<Vec<Bytes>>>`
  - `fn get_metadata<'a>(self: &'a  mut Self, options: Option<&'a ArrowReaderOptions>) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>>`



## datafusion_datasource_parquet::reader::CachedParquetFileReaderFactory

*Struct*

Implementation of [`ParquetFileReaderFactory`] supporting the caching of footer and page
metadata. Reads and updates the [`FileMetadataCache`] with the [`ParquetMetaData`] data.
This reader always loads the entire metadata (including page index, unless the file is
encrypted), even if not required by the current query, to ensure it is always available for
those that need it.

**Methods:**

- `fn new(store: Arc<dyn ObjectStore>, metadata_cache: Arc<dyn FileMetadataCache>) -> Self`

**Trait Implementations:**

- **ParquetFileReaderFactory**
  - `fn create_reader(self: &Self, partition_index: usize, partitioned_file: PartitionedFile, metadata_size_hint: Option<usize>, metrics: &ExecutionPlanMetricsSet) -> datafusion_common::Result<Box<dyn AsyncFileReader>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource_parquet::reader::CachedParquetMetaData

*Struct*

Wrapper to implement [`FileMetadata`] for [`ParquetMetaData`].

**Tuple Struct**: `()`

**Methods:**

- `fn new(metadata: Arc<ParquetMetaData>) -> Self`
- `fn parquet_metadata(self: &Self) -> &Arc<ParquetMetaData>`

**Trait Implementations:**

- **FileMetadata**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn memory_size(self: &Self) -> usize`
  - `fn extra_info(self: &Self) -> HashMap<String, String>`



## datafusion_datasource_parquet::reader::DefaultParquetFileReaderFactory

*Struct*

Default implementation of [`ParquetFileReaderFactory`]

This implementation:
1. Reads parquet directly from an underlying [`ObjectStore`] instance.
2. Reads the footer and page metadata on demand.
3. Does not cache metadata or coalesce I/O operations.

**Methods:**

- `fn new(store: Arc<dyn ObjectStore>) -> Self` - Create a new `DefaultParquetFileReaderFactory`.

**Trait Implementations:**

- **ParquetFileReaderFactory**
  - `fn create_reader(self: &Self, partition_index: usize, partitioned_file: PartitionedFile, metadata_size_hint: Option<usize>, metrics: &ExecutionPlanMetricsSet) -> datafusion_common::Result<Box<dyn AsyncFileReader>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource_parquet::reader::ParquetFileReader

*Struct*

Implements [`AsyncFileReader`] for a parquet file in object storage.

This implementation uses the [`ParquetObjectReader`] to read data from the
object store on demand, as required, tracking the number of bytes read.

This implementation does not coalesce I/O operations or cache bytes. Such
optimizations can be done either at the object store level or by providing a
custom implementation of [`ParquetFileReaderFactory`].

**Fields:**
- `file_metrics: crate::ParquetFileMetrics`
- `inner: parquet::arrow::async_reader::ParquetObjectReader`
- `partitioned_file: datafusion_datasource::PartitionedFile`

**Trait Implementations:**

- **Drop**
  - `fn drop(self: & mut Self)`
- **AsyncFileReader**
  - `fn get_bytes(self: & mut Self, range: Range<u64>) -> BoxFuture<parquet::errors::Result<Bytes>>`
  - `fn get_byte_ranges(self: & mut Self, ranges: Vec<Range<u64>>) -> BoxFuture<parquet::errors::Result<Vec<Bytes>>>`
  - `fn get_metadata<'a>(self: &'a  mut Self, options: Option<&'a ArrowReaderOptions>) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>>`



## datafusion_datasource_parquet::reader::ParquetFileReaderFactory

*Trait*

Interface for reading parquet files.

The combined implementations of [`ParquetFileReaderFactory`] and
[`AsyncFileReader`] can be used to provide custom data access operations
such as pre-cached metadata, I/O coalescing, etc.

See [`DefaultParquetFileReaderFactory`] for a simple implementation.

**Methods:**

- `create_reader`: Provides an `AsyncFileReader` for reading data from a parquet file specified



