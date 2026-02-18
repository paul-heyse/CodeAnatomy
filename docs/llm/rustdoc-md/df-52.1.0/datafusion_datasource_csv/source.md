**datafusion_datasource_csv > source**

# Module: source

## Contents

**Structs**

- [`CsvOpener`](#csvopener) - A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]
- [`CsvSource`](#csvsource) - A Config for [`CsvOpener`]

**Functions**

- [`plan_to_csv`](#plan_to_csv)

---

## datafusion_datasource_csv::source::CsvOpener

*Struct*

A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]

**Methods:**

- `fn new(config: Arc<CsvSource>, file_compression_type: FileCompressionType, object_store: Arc<dyn ObjectStore>) -> Self` - Returns a [`CsvOpener`]

**Trait Implementations:**

- **FileOpener**
  - `fn open(self: &Self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture>` - Open a partitioned CSV file.



## datafusion_datasource_csv::source::CsvSource

*Struct*

A Config for [`CsvOpener`]

# Example: create a `DataSourceExec` for CSV
```
# use std::sync::Arc;
# use arrow::datatypes::Schema;
# use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
# use datafusion_datasource::PartitionedFile;
# use datafusion_datasource_csv::source::CsvSource;
# use datafusion_execution::object_store::ObjectStoreUrl;
# use datafusion_datasource::source::DataSourceExec;
# use datafusion_common::config::CsvOptions;

# let object_store_url = ObjectStoreUrl::local_filesystem();
# let file_schema = Arc::new(Schema::empty());

let options = CsvOptions {
    has_header: Some(true),
    delimiter: b',',
    quote: b'"',
    newlines_in_values: Some(true), // The file contains newlines in values
    ..Default::default()
};
let source = Arc::new(CsvSource::new(file_schema.clone())
    .with_csv_options(options)
    .with_terminator(Some(b'#'))
);
// Create a DataSourceExec for reading the first 100MB of `file1.csv`
let config = FileScanConfigBuilder::new(object_store_url, source)
    .with_file(PartitionedFile::new("file1.csv", 100*1024*1024))
    .build();
let exec = (DataSourceExec::from_data_source(config));
```

**Methods:**

- `fn new<impl Into<TableSchema>>(table_schema: impl Trait) -> Self` - Returns a [`CsvSource`]
- `fn with_csv_options(self: Self, options: CsvOptions) -> Self` - Sets the CSV options
- `fn has_header(self: &Self) -> bool` - true if the first line of each file is a header
- `fn truncate_rows(self: &Self) -> bool`
- `fn delimiter(self: &Self) -> u8` - A column delimiter
- `fn quote(self: &Self) -> u8` - The quote character
- `fn terminator(self: &Self) -> Option<u8>` - The line terminator
- `fn comment(self: &Self) -> Option<u8>` - Lines beginning with this byte are ignored.
- `fn escape(self: &Self) -> Option<u8>` - The escape character
- `fn with_escape(self: &Self, escape: Option<u8>) -> Self` - Initialize a CsvSource with escape
- `fn with_terminator(self: &Self, terminator: Option<u8>) -> Self` - Initialize a CsvSource with terminator
- `fn with_comment(self: &Self, comment: Option<u8>) -> Self` - Initialize a CsvSource with comment
- `fn with_truncate_rows(self: &Self, truncate_rows: bool) -> Self` - Whether to support truncate rows when read csv file
- `fn newlines_in_values(self: &Self) -> bool` - Whether values may contain newline characters

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CsvSource`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FileSource**
  - `fn create_file_opener(self: &Self, object_store: Arc<dyn ObjectStore>, base_config: &FileScanConfig, partition_index: usize) -> Result<Arc<dyn FileOpener>>`
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn table_schema(self: &Self) -> &TableSchema`
  - `fn with_batch_size(self: &Self, batch_size: usize) -> Arc<dyn FileSource>`
  - `fn try_pushdown_projection(self: &Self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn FileSource>>>`
  - `fn projection(self: &Self) -> Option<&ProjectionExprs>`
  - `fn metrics(self: &Self) -> &ExecutionPlanMetricsSet`
  - `fn file_type(self: &Self) -> &str`
  - `fn supports_repartitioning(self: &Self) -> bool`
  - `fn fmt_extra(self: &Self, t: DisplayFormatType, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_datasource_csv::source::plan_to_csv

*Function*

```rust
fn plan_to_csv<impl AsRef<str>>(task_ctx: std::sync::Arc<datafusion_execution::TaskContext>, plan: std::sync::Arc<dyn ExecutionPlan>, path: impl Trait) -> datafusion_common::Result<()>
```



