**datafusion_datasource > file_format**

# Module: file_format

## Contents

**Structs**

- [`DefaultFileType`](#defaultfiletype) - A container of [FileFormatFactory] which also implements [FileType].

**Functions**

- [`file_type_to_format`](#file_type_to_format) - Converts a [FileType] to a [FileFormatFactory].
- [`format_as_file_type`](#format_as_file_type) - Converts a [FileFormatFactory] to a [FileType]

**Traits**

- [`FileFormat`](#fileformat) - This trait abstracts all the file format specific implementations
- [`FileFormatFactory`](#fileformatfactory) - Factory for creating [`FileFormat`] instances based on session and command level options

**Constants**

- [`DEFAULT_SCHEMA_INFER_MAX_RECORD`](#default_schema_infer_max_record) - Default max records to scan to infer the schema

---

## datafusion_datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD

*Constant*: `usize`

Default max records to scan to infer the schema



## datafusion_datasource::file_format::DefaultFileType

*Struct*

A container of [FileFormatFactory] which also implements [FileType].
This enables converting a dyn FileFormat to a dyn FileType.
The former trait is a superset of the latter trait, which includes execution time
relevant methods. [FileType] is only used in logical planning and only implements
the subset of methods required during logical planning.

**Methods:**

- `fn new(file_format_factory: Arc<dyn FileFormatFactory>) -> Self` - Constructs a [DefaultFileType] wrapper from a [FileFormatFactory]
- `fn as_format_factory(self: &Self) -> &Arc<dyn FileFormatFactory>` - get a reference to the inner [FileFormatFactory] struct

**Trait Implementations:**

- **GetExt**
  - `fn get_ext(self: &Self) -> String`
- **FileType**
  - `fn as_any(self: &Self) -> &dyn Any`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_datasource::file_format::FileFormat

*Trait*

This trait abstracts all the file format specific implementations
from the [`TableProvider`]. This helps code re-utilization across
providers that support the same file formats.

[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

**Methods:**

- `as_any`: Returns the table provider as [`Any`] so that it can be
- `get_ext`: Returns the extension for this FileFormat, e.g. "file.csv" -> csv
- `get_ext_with_compression`: Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
- `compression_type`: Returns whether this instance uses compression if applicable
- `infer_schema`: Infer the common schema of the provided objects. The objects will usually
- `infer_stats`: Infer the statistics for the provided object. The cost and accuracy of the
- `create_physical_plan`: Take a list of files and convert it to the appropriate executor
- `create_writer_physical_plan`: Take a list of files and the configuration to convert it to the
- `file_source`: Return the related FileSource such as `CsvSource`, `JsonSource`, etc.



## datafusion_datasource::file_format::FileFormatFactory

*Trait*

Factory for creating [`FileFormat`] instances based on session and command level options

Users can provide their own `FileFormatFactory` to support arbitrary file formats

**Methods:**

- `create`: Initialize a [FileFormat] and configure based on session and command level options
- `default`: Initialize a [FileFormat] with all options set to default values
- `as_any`: Returns the table source as [`Any`] so that it can be



## datafusion_datasource::file_format::file_type_to_format

*Function*

Converts a [FileType] to a [FileFormatFactory].
Returns an error if the [FileType] cannot be
downcasted to a [DefaultFileType].

```rust
fn file_type_to_format(file_type: &std::sync::Arc<dyn FileType>) -> datafusion_common::Result<std::sync::Arc<dyn FileFormatFactory>>
```



## datafusion_datasource::file_format::format_as_file_type

*Function*

Converts a [FileFormatFactory] to a [FileType]

```rust
fn format_as_file_type(file_format_factory: std::sync::Arc<dyn FileFormatFactory>) -> std::sync::Arc<dyn FileType>
```



