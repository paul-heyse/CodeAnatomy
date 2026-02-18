**datafusion_common > file_options > csv_writer**

# Module: file_options::csv_writer

## Contents

**Structs**

- [`CsvWriterOptions`](#csvwriteroptions) - Options for writing CSV files

---

## datafusion_common::file_options::csv_writer::CsvWriterOptions

*Struct*

Options for writing CSV files

**Fields:**
- `writer_options: arrow::csv::WriterBuilder` - Struct from the arrow crate which contains all csv writing related settings
- `compression: crate::parsers::CompressionTypeVariant` - Compression to apply after ArrowWriter serializes RecordBatches.
- `compression_level: Option<u32>` - Compression level for the output file.

**Methods:**

- `fn new(writer_options: WriterBuilder, compression: CompressionTypeVariant) -> Self`
- `fn new_with_level(writer_options: WriterBuilder, compression: CompressionTypeVariant, compression_level: u32) -> Self` - Create a new `CsvWriterOptions` with the specified compression level.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CsvWriterOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(value: &CsvOptions) -> Result<Self>`



