**datafusion_datasource_parquet > metadata**

# Module: metadata

## Contents

**Structs**

- [`CachedParquetMetaData`](#cachedparquetmetadata) - Wrapper to implement [`FileMetadata`] for [`ParquetMetaData`].
- [`DFParquetMetadata`](#dfparquetmetadata) - Handles fetching Parquet file schema, metadata and statistics

---

## datafusion_datasource_parquet::metadata::CachedParquetMetaData

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



## datafusion_datasource_parquet::metadata::DFParquetMetadata

*Struct*

Handles fetching Parquet file schema, metadata and statistics
from object store.

This component is exposed for low level integrations through
[`ParquetFileReaderFactory`].

[`ParquetFileReaderFactory`]: crate::ParquetFileReaderFactory

**Generic Parameters:**
- 'a

**Fields:**
- `coerce_int96: Option<arrow::datatypes::TimeUnit>` - timeunit to coerce INT96 timestamps to

**Methods:**

- `fn new(store: &'a dyn ObjectStore, object_meta: &'a ObjectMeta) -> Self`
- `fn with_metadata_size_hint(self: Self, metadata_size_hint: Option<usize>) -> Self` - set metadata size hint
- `fn with_decryption_properties(self: Self, decryption_properties: Option<Arc<FileDecryptionProperties>>) -> Self` - set decryption properties
- `fn with_file_metadata_cache(self: Self, file_metadata_cache: Option<Arc<dyn FileMetadataCache>>) -> Self` - set file metadata cache
- `fn with_coerce_int96(self: Self, time_unit: Option<TimeUnit>) -> Self` - Set timeunit to coerce INT96 timestamps to
- `fn fetch_metadata(self: &Self) -> Result<Arc<ParquetMetaData>>` - Fetch parquet metadata from the remote object store
- `fn fetch_schema(self: &Self) -> Result<Schema>` - Read and parse the schema of the Parquet file
- `fn fetch_statistics(self: &Self, table_schema: &SchemaRef) -> Result<Statistics>` - Fetch the metadata from the Parquet file via [`Self::fetch_metadata`] and convert
- `fn statistics_from_parquet_metadata(metadata: &ParquetMetaData, logical_file_schema: &SchemaRef) -> Result<Statistics>` - Convert statistics in [`ParquetMetaData`] into [`Statistics`] using [`StatisticsConverter`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



