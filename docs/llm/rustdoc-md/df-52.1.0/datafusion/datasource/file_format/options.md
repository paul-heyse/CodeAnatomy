**datafusion > datasource > file_format > options**

# Module: datasource::file_format::options

## Contents

**Structs**

- [`ArrowReadOptions`](#arrowreadoptions) - Options that control the reading of ARROW files.
- [`AvroReadOptions`](#avroreadoptions) - Options that control the reading of AVRO files.
- [`CsvReadOptions`](#csvreadoptions) - Options that control the reading of CSV files.
- [`NdJsonReadOptions`](#ndjsonreadoptions) - Options that control the reading of Line-delimited JSON files (NDJson)
- [`ParquetReadOptions`](#parquetreadoptions) - Options that control the reading of Parquet files.

**Traits**

- [`ReadOptions`](#readoptions) - ['ReadOptions'] is implemented by Options like ['CsvReadOptions'] that control the reading of respective files/sources.

---

## datafusion::datasource::file_format::options::ArrowReadOptions

*Struct*

Options that control the reading of ARROW files.

Note this structure is supplied when a datasource is created and
can not not vary from statement to statement. For settings that
can vary statement to statement see
[`ConfigOptions`](crate::config::ConfigOptions).

**Generic Parameters:**
- 'a

**Fields:**
- `schema: Option<&'a arrow::datatypes::Schema>` - The data source schema.
- `file_extension: &'a str` - File extension; only files with this extension are selected for data input.
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - Partition Columns

**Methods:**

- `fn table_partition_cols(self: Self, table_partition_cols: Vec<(String, DataType)>) -> Self` - Specify table_partition_cols for partition pruning
- `fn schema(self: Self, schema: &'a Schema) -> Self` - Specify schema to use for AVRO read

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **ReadOptions**
  - `fn to_listing_options(self: &Self, config: &SessionConfig, _table_options: TableOptions) -> ListingOptions`
  - `fn get_resolved_schema(self: &'life0 Self, config: &'life1 SessionConfig, state: SessionState, table_path: ListingTableUrl) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Clone**
  - `fn clone(self: &Self) -> ArrowReadOptions<'a>`



## datafusion::datasource::file_format::options::AvroReadOptions

*Struct*

Options that control the reading of AVRO files.

Note this structure is supplied when a datasource is created and
can not not vary from statement to statement. For settings that
can vary statement to statement see
[`ConfigOptions`](crate::config::ConfigOptions).

**Generic Parameters:**
- 'a

**Fields:**
- `schema: Option<&'a arrow::datatypes::Schema>` - The data source schema.
- `file_extension: &'a str` - File extension; only files with this extension are selected for data input.
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - Partition Columns

**Methods:**

- `fn table_partition_cols(self: Self, table_partition_cols: Vec<(String, DataType)>) -> Self` - Specify table_partition_cols for partition pruning
- `fn schema(self: Self, schema: &'a Schema) -> Self` - Specify schema to use for AVRO read

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> AvroReadOptions<'a>`
- **Default**
  - `fn default() -> Self`



## datafusion::datasource::file_format::options::CsvReadOptions

*Struct*

Options that control the reading of CSV files.

Note this structure is supplied when a datasource is created and
can not not vary from statement to statement. For settings that
can vary statement to statement see
[`ConfigOptions`](crate::config::ConfigOptions).

**Generic Parameters:**
- 'a

**Fields:**
- `has_header: bool` - Does the CSV file have a header?
- `delimiter: u8` - An optional column delimiter. Defaults to `b','`.
- `quote: u8` - An optional quote character. Defaults to `b'"'`.
- `terminator: Option<u8>` - An optional terminator character. Defaults to None (CRLF).
- `escape: Option<u8>` - An optional escape character. Defaults to None.
- `comment: Option<u8>` - If enabled, lines beginning with this byte are ignored.
- `newlines_in_values: bool` - Specifies whether newlines in (quoted) values are supported.
- `schema: Option<&'a arrow::datatypes::Schema>` - An optional schema representing the CSV files. If None, CSV reader will try to infer it
- `schema_infer_max_records: usize` - Max number of rows to read from CSV files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
- `file_extension: &'a str` - File extension; only files with this extension are selected for data input.
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - Partition Columns
- `file_compression_type: crate::datasource::file_format::file_compression_type::FileCompressionType` - File compression type
- `file_sort_order: Vec<Vec<datafusion_expr::SortExpr>>` - Indicates how the file is sorted
- `null_regex: Option<String>` - Optional regex to match null values
- `truncated_rows: bool` - Whether to allow truncated rows when parsing.

**Methods:**

- `fn new() -> Self` - Create a CSV read option with default presets
- `fn has_header(self: Self, has_header: bool) -> Self` - Configure has_header setting
- `fn comment(self: Self, comment: u8) -> Self` - Specify comment char to use for CSV read
- `fn delimiter(self: Self, delimiter: u8) -> Self` - Specify delimiter to use for CSV read
- `fn quote(self: Self, quote: u8) -> Self` - Specify quote to use for CSV read
- `fn terminator(self: Self, terminator: Option<u8>) -> Self` - Specify terminator to use for CSV read
- `fn escape(self: Self, escape: u8) -> Self` - Specify delimiter to use for CSV read
- `fn newlines_in_values(self: Self, newlines_in_values: bool) -> Self` - Specifies whether newlines in (quoted) values are supported.
- `fn file_extension(self: Self, file_extension: &'a str) -> Self` - Specify the file extension for CSV file selection
- `fn delimiter_option(self: Self, delimiter: Option<u8>) -> Self` - Configure delimiter setting with Option, None value will be ignored
- `fn schema(self: Self, schema: &'a Schema) -> Self` - Specify schema to use for CSV read
- `fn table_partition_cols(self: Self, table_partition_cols: Vec<(String, DataType)>) -> Self` - Specify table_partition_cols for partition pruning
- `fn schema_infer_max_records(self: Self, max_records: usize) -> Self` - Configure number of max records to read for schema inference
- `fn file_compression_type(self: Self, file_compression_type: FileCompressionType) -> Self` - Configure file compression type
- `fn file_sort_order(self: Self, file_sort_order: Vec<Vec<SortExpr>>) -> Self` - Configure if file has known sort order
- `fn null_regex(self: Self, null_regex: Option<String>) -> Self` - Configure the null parsing regex.
- `fn truncated_rows(self: Self, truncated_rows: bool) -> Self` - Configure whether to allow truncated rows when parsing.

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **ReadOptions**
  - `fn to_listing_options(self: &Self, config: &SessionConfig, table_options: TableOptions) -> ListingOptions`
  - `fn get_resolved_schema(self: &'life0 Self, config: &'life1 SessionConfig, state: SessionState, table_path: ListingTableUrl) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Clone**
  - `fn clone(self: &Self) -> CsvReadOptions<'a>`



## datafusion::datasource::file_format::options::NdJsonReadOptions

*Struct*

Options that control the reading of Line-delimited JSON files (NDJson)

Note this structure is supplied when a datasource is created and
can not not vary from statement to statement. For settings that
can vary statement to statement see
[`ConfigOptions`](crate::config::ConfigOptions).

**Generic Parameters:**
- 'a

**Fields:**
- `schema: Option<&'a arrow::datatypes::Schema>` - The data source schema.
- `schema_infer_max_records: usize` - Max number of rows to read from JSON files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
- `file_extension: &'a str` - File extension; only files with this extension are selected for data input.
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - Partition Columns
- `file_compression_type: crate::datasource::file_format::file_compression_type::FileCompressionType` - File compression type
- `infinite: bool` - Flag indicating whether this file may be unbounded (as in a FIFO file).
- `file_sort_order: Vec<Vec<datafusion_expr::SortExpr>>` - Indicates how the file is sorted

**Methods:**

- `fn table_partition_cols(self: Self, table_partition_cols: Vec<(String, DataType)>) -> Self` - Specify table_partition_cols for partition pruning
- `fn file_extension(self: Self, file_extension: &'a str) -> Self` - Specify file_extension
- `fn mark_infinite(self: Self, infinite: bool) -> Self` - Configure mark_infinite setting
- `fn file_compression_type(self: Self, file_compression_type: FileCompressionType) -> Self` - Specify file_compression_type
- `fn schema(self: Self, schema: &'a Schema) -> Self` - Specify schema to use for NdJson read
- `fn file_sort_order(self: Self, file_sort_order: Vec<Vec<SortExpr>>) -> Self` - Configure if file has known sort order
- `fn schema_infer_max_records(self: Self, schema_infer_max_records: usize) -> Self` - Specify how many rows to read for schema inference

**Trait Implementations:**

- **ReadOptions**
  - `fn to_listing_options(self: &Self, config: &SessionConfig, table_options: TableOptions) -> ListingOptions`
  - `fn get_resolved_schema(self: &'life0 Self, config: &'life1 SessionConfig, state: SessionState, table_path: ListingTableUrl) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Clone**
  - `fn clone(self: &Self) -> NdJsonReadOptions<'a>`
- **Default**
  - `fn default() -> Self`



## datafusion::datasource::file_format::options::ParquetReadOptions

*Struct*

Options that control the reading of Parquet files.

Note this structure is supplied when a datasource is created and
can not not vary from statement to statement. For settings that
can vary statement to statement see
[`ConfigOptions`](crate::config::ConfigOptions).

**Generic Parameters:**
- 'a

**Fields:**
- `file_extension: &'a str` - File extension; only files with this extension are selected for data input.
- `table_partition_cols: Vec<(String, arrow::datatypes::DataType)>` - Partition Columns
- `parquet_pruning: Option<bool>` - Should the parquet reader use the predicate to prune row groups?
- `skip_metadata: Option<bool>` - Should the parquet reader to skip any metadata that may be in
- `schema: Option<&'a arrow::datatypes::Schema>` - An optional schema representing the parquet files. If None, parquet reader will try to infer it
- `file_sort_order: Vec<Vec<datafusion_expr::SortExpr>>` - Indicates how the file is sorted
- `file_decryption_properties: Option<datafusion_common::config::ConfigFileDecryptionProperties>` - Properties for decryption of Parquet files that use modular encryption
- `metadata_size_hint: Option<usize>` - Metadata size hint for Parquet files reading (in bytes)

**Methods:**

- `fn new() -> Self` - Create a new ParquetReadOptions with default values
- `fn file_extension(self: Self, file_extension: &'a str) -> Self` - Specify file_extension
- `fn parquet_pruning(self: Self, parquet_pruning: bool) -> Self` - Specify parquet_pruning
- `fn skip_metadata(self: Self, skip_metadata: bool) -> Self` - Tell the parquet reader to skip any metadata that may be in
- `fn schema(self: Self, schema: &'a Schema) -> Self` - Specify schema to use for parquet read
- `fn table_partition_cols(self: Self, table_partition_cols: Vec<(String, DataType)>) -> Self` - Specify table_partition_cols for partition pruning
- `fn file_sort_order(self: Self, file_sort_order: Vec<Vec<SortExpr>>) -> Self` - Configure if file has known sort order
- `fn file_decryption_properties(self: Self, file_decryption_properties: ConfigFileDecryptionProperties) -> Self` - Configure file decryption properties for reading encrypted Parquet files
- `fn metadata_size_hint(self: Self, size_hint: Option<usize>) -> Self` - Configure metadata size hint for Parquet files reading (in bytes)

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **ReadOptions**
  - `fn to_listing_options(self: &Self, config: &SessionConfig, table_options: TableOptions) -> ListingOptions`
  - `fn get_resolved_schema(self: &'life0 Self, config: &'life1 SessionConfig, state: SessionState, table_path: ListingTableUrl) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Clone**
  - `fn clone(self: &Self) -> ParquetReadOptions<'a>`



## datafusion::datasource::file_format::options::ReadOptions

*Trait*

['ReadOptions'] is implemented by Options like ['CsvReadOptions'] that control the reading of respective files/sources.

**Methods:**

- `to_listing_options`: Helper to convert these user facing options to `ListingTable` options
- `get_resolved_schema`: Infer and resolve the schema from the files/sources provided.
- `_get_resolved_schema`: helper function to reduce repetitive code. Infers the schema from sources if not provided. Infinite data sources not supported through this function.



