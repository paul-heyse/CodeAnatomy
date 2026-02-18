**datafusion_datasource_csv > file_format**

# Module: file_format

## Contents

**Structs**

- [`CsvDecoder`](#csvdecoder)
- [`CsvFormat`](#csvformat) - Character Separated Value [`FileFormat`] implementation.
- [`CsvFormatFactory`](#csvformatfactory) - Factory used to create [`CsvFormat`]
- [`CsvSerializer`](#csvserializer) - Define a struct for serializing CSV records to a stream
- [`CsvSink`](#csvsink) - Implements [`DataSink`] for writing to a CSV file.

---

## datafusion_datasource_csv::file_format::CsvDecoder

*Struct*

**Methods:**

- `fn new(decoder: arrow::csv::reader::Decoder) -> Self`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Decoder**
  - `fn decode(self: & mut Self, buf: &[u8]) -> Result<usize, ArrowError>`
  - `fn flush(self: & mut Self) -> Result<Option<RecordBatch>, ArrowError>`
  - `fn can_flush_early(self: &Self) -> bool`



## datafusion_datasource_csv::file_format::CsvFormat

*Struct*

Character Separated Value [`FileFormat`] implementation.

**Methods:**

- `fn infer_schema_from_stream<impl Stream<Item = Result<Bytes>>>(self: &Self, state: &dyn Session, records_to_read: usize, stream: impl Trait) -> Result<(Schema, usize)>` - Return the inferred schema reading up to records_to_read from a
- `fn read_to_delimited_chunks_from_stream<'a>(self: &Self, stream: BoxStream<'a, Result<Bytes>>) -> BoxStream<'a, Result<Bytes>>` - Convert a stream of bytes into a stream of of [`Bytes`] containing newline
- `fn with_options(self: Self, options: CsvOptions) -> Self` - Set the csv options
- `fn options(self: &Self) -> &CsvOptions` - Retrieve the csv options
- `fn with_schema_infer_max_rec(self: Self, max_rec: usize) -> Self` - Set a limit in terms of records to scan to infer the schema
- `fn with_has_header(self: Self, has_header: bool) -> Self` - Set true to indicate that the first line is a header.
- `fn with_truncated_rows(self: Self, truncated_rows: bool) -> Self`
- `fn with_null_regex(self: Self, null_regex: Option<String>) -> Self` - Set the regex to use for null values in the CSV reader.
- `fn has_header(self: &Self) -> Option<bool>` - Returns `Some(true)` if the first line is a header, `Some(false)` if
- `fn with_comment(self: Self, comment: Option<u8>) -> Self` - Lines beginning with this byte are ignored.
- `fn with_delimiter(self: Self, delimiter: u8) -> Self` - The character separating values within a row.
- `fn with_quote(self: Self, quote: u8) -> Self` - The quote character in a row.
- `fn with_escape(self: Self, escape: Option<u8>) -> Self` - The escape character in a row.
- `fn with_terminator(self: Self, terminator: Option<u8>) -> Self` - The character used to indicate the end of a row.
- `fn with_newlines_in_values(self: Self, newlines_in_values: bool) -> Self` - Specifies whether newlines in (quoted) values are supported.
- `fn with_file_compression_type(self: Self, file_compression_type: FileCompressionType) -> Self` - Set a `FileCompressionType` of CSV
- `fn with_truncate_rows(self: Self, truncate_rows: bool) -> Self` - Set whether rows should be truncated to the column width
- `fn delimiter(self: &Self) -> u8` - The delimiter character.
- `fn quote(self: &Self) -> u8` - The quote character.
- `fn escape(self: &Self) -> Option<u8>` - The escape character.

**Trait Implementations:**

- **Default**
  - `fn default() -> CsvFormat`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FileFormat**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn get_ext(self: &Self) -> String`
  - `fn get_ext_with_compression(self: &Self, file_compression_type: &FileCompressionType) -> Result<String>`
  - `fn compression_type(self: &Self) -> Option<FileCompressionType>`
  - `fn infer_schema(self: &'life0 Self, state: &'life1 dyn Session, store: &'life2 Arc<dyn ObjectStore>, objects: &'life3 [ObjectMeta]) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn infer_stats(self: &'life0 Self, _state: &'life1 dyn Session, _store: &'life2 Arc<dyn ObjectStore>, table_schema: SchemaRef, _object: &'life3 ObjectMeta) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn create_physical_plan(self: &'life0 Self, state: &'life1 dyn Session, conf: FileScanConfig) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn create_writer_physical_plan(self: &'life0 Self, input: Arc<dyn ExecutionPlan>, state: &'life1 dyn Session, conf: FileSinkConfig, order_requirements: Option<LexRequirement>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn file_source(self: &Self, table_schema: TableSchema) -> Arc<dyn FileSource>`



## datafusion_datasource_csv::file_format::CsvFormatFactory

*Struct*

Factory used to create [`CsvFormat`]

**Fields:**
- `options: Option<datafusion_common::config::CsvOptions>` - the options for csv file read

**Methods:**

- `fn new() -> Self` - Creates an instance of [`CsvFormatFactory`]
- `fn new_with_options(options: CsvOptions) -> Self` - Creates an instance of [`CsvFormatFactory`] with customized default options

**Trait Implementations:**

- **Default**
  - `fn default() -> CsvFormatFactory`
- **GetExt**
  - `fn get_ext(self: &Self) -> String`
- **FileFormatFactory**
  - `fn create(self: &Self, state: &dyn Session, format_options: &HashMap<String, String>) -> Result<Arc<dyn FileFormat>>`
  - `fn default(self: &Self) -> Arc<dyn FileFormat>`
  - `fn as_any(self: &Self) -> &dyn Any`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_datasource_csv::file_format::CsvSerializer

*Struct*

Define a struct for serializing CSV records to a stream

**Methods:**

- `fn new() -> Self` - Constructor for the CsvSerializer object
- `fn with_builder(self: Self, builder: WriterBuilder) -> Self` - Method for setting the CSV writer builder
- `fn with_header(self: Self, header: bool) -> Self` - Method for setting the CSV writer header status

**Trait Implementations:**

- **BatchSerializer**
  - `fn serialize(self: &Self, batch: RecordBatch, initial: bool) -> Result<Bytes>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_datasource_csv::file_format::CsvSink

*Struct*

Implements [`DataSink`] for writing to a CSV file.

**Methods:**

- `fn new(config: FileSinkConfig, writer_options: CsvWriterOptions) -> Self` - Create from config.
- `fn writer_options(self: &Self) -> &CsvWriterOptions` - Retrieve the writer options

**Trait Implementations:**

- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **DataSink**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> &SchemaRef`
  - `fn write_all(self: &'life0 Self, data: SendableRecordBatchStream, context: &'life1 Arc<TaskContext>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **FileSink**
  - `fn config(self: &Self) -> &FileSinkConfig`
  - `fn spawn_writer_tasks_and_join(self: &'life0 Self, context: &'life1 Arc<TaskContext>, demux_task: SpawnedTask<Result<()>>, file_stream_rx: DemuxedStreamReceiver, object_store: Arc<dyn ObjectStore>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



