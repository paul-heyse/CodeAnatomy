**datafusion_common > file_options > parquet_writer**

# Module: file_options::parquet_writer

## Contents

**Structs**

- [`ParquetWriterOptions`](#parquetwriteroptions) - Options for writing parquet files

**Functions**

- [`parse_compression_string`](#parse_compression_string) - Parses datafusion.execution.parquet.compression String to a parquet::basic::Compression

---

## datafusion_common::file_options::parquet_writer::ParquetWriterOptions

*Struct*

Options for writing parquet files

**Fields:**
- `writer_options: parquet::file::properties::WriterProperties` - parquet-rs writer properties

**Methods:**

- `fn new(writer_options: WriterProperties) -> Self`
- `fn writer_options(self: &Self) -> &WriterProperties`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ParquetWriterOptions`
- **TryFrom**
  - `fn try_from(parquet_table_options: &TableParquetOptions) -> Result<Self>`



## datafusion_common::file_options::parquet_writer::parse_compression_string

*Function*

Parses datafusion.execution.parquet.compression String to a parquet::basic::Compression

```rust
fn parse_compression_string(str_setting: &str) -> crate::Result<parquet::basic::Compression>
```



