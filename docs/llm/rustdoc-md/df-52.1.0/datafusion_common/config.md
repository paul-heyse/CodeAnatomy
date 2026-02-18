**datafusion_common > config**

# Module: config

## Contents

**Structs**

- [`CatalogOptions`](#catalogoptions) - Options related to catalog and directory scanning
- [`ColumnDecryptionProperties`](#columndecryptionproperties)
- [`ColumnEncryptionProperties`](#columnencryptionproperties)
- [`ConfigEntry`](#configentry) - A key value pair, with a corresponding description
- [`ConfigFileDecryptionProperties`](#configfiledecryptionproperties)
- [`ConfigFileEncryptionProperties`](#configfileencryptionproperties)
- [`ConfigOptions`](#configoptions) - Configuration options struct, able to store both built-in configuration and custom options
- [`CsvOptions`](#csvoptions) - Options controlling CSV format
- [`EncryptionFactoryOptions`](#encryptionfactoryoptions) - Holds implementation-specific options for an encryption factory
- [`ExecutionOptions`](#executionoptions) - Options related to query execution
- [`ExplainOptions`](#explainoptions) - Options controlling explain output
- [`Extensions`](#extensions) - A type-safe container for [`ConfigExtension`]
- [`FormatOptions`](#formatoptions) - Options controlling the format of output when printing record batches
- [`JsonOptions`](#jsonoptions) - Options controlling JSON format
- [`OptimizerOptions`](#optimizeroptions) - Options related to query optimization
- [`ParquetColumnOptions`](#parquetcolumnoptions) - Options controlling parquet format for individual columns.
- [`ParquetEncryptionOptions`](#parquetencryptionoptions) - Options for configuring Parquet Modular Encryption
- [`ParquetOptions`](#parquetoptions) - Options for reading and writing parquet files
- [`SqlParserOptions`](#sqlparseroptions) - Options related to SQL parser
- [`TableOptions`](#tableoptions) - Represents the configuration options available for handling different table formats within a data processing application.
- [`TableParquetOptions`](#tableparquetoptions) - Options that control how Parquet files are read, including global options

**Enums**

- [`ConfigFileType`](#configfiletype) - These file types have special built in behavior for configuration.
- [`Dialect`](#dialect) - This is the SQL dialect used by DataFusion's parser.
- [`OutputFormat`](#outputformat)
- [`SpillCompression`](#spillcompression)

**Functions**

- [`default_config_transform`](#default_config_transform) - Default transformation to parse a [`ConfigField`] for a string.

**Traits**

- [`ConfigExtension`](#configextension) - [`ConfigExtension`] provides a mechanism to store third-party configuration
- [`ConfigField`](#configfield) - A trait implemented by `config_namespace` and for field types that provides
- [`ExtensionOptions`](#extensionoptions) - An object-safe API for storing arbitrary configuration.
- [`OutputFormatExt`](#outputformatext)
- [`Visit`](#visit) - An implementation trait used to recursively walk configuration

---

## datafusion_common::config::CatalogOptions

*Struct*

Options related to catalog and directory scanning

See also: [`SessionConfig`]

[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html

**Fields:**
- `create_default_catalog_and_schema: bool` - Whether the default catalog and schema should be created automatically.
- `default_catalog: String` - The default catalog name - this impacts what SQL queries use if not specified
- `default_schema: String` - The default schema name - this impacts what SQL queries use if not specified
- `information_schema: bool` - Should DataFusion provide access to `information_schema`
- `location: Option<String>` - Location scanned to load tables for `default` schema
- `format: Option<String>` - Type of `TableProvider` to use when loading `default` schema
- `has_header: bool` - Default value for `format.has_header` for `CREATE EXTERNAL TABLE`
- `newlines_in_values: bool` - Specifies whether newlines in (quoted) CSV values are supported.

**Trait Implementations:**

- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CatalogOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &CatalogOptions) -> bool`



## datafusion_common::config::ColumnDecryptionProperties

*Struct*

**Fields:**
- `column_key_as_hex: String` - Per column encryption key

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ColumnDecryptionProperties`
- **PartialEq**
  - `fn eq(self: &Self, other: &ColumnDecryptionProperties) -> bool`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`



## datafusion_common::config::ColumnEncryptionProperties

*Struct*

**Fields:**
- `column_key_as_hex: String` - Per column encryption key
- `column_metadata_as_hex: Option<String>` - Per column encryption key metadata

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
- **Clone**
  - `fn clone(self: &Self) -> ColumnEncryptionProperties`
- **PartialEq**
  - `fn eq(self: &Self, other: &ColumnEncryptionProperties) -> bool`
- **Default**
  - `fn default() -> Self`



## datafusion_common::config::ConfigEntry

*Struct*

A key value pair, with a corresponding description

**Fields:**
- `key: String` - A unique string to identify this config value
- `value: Option<String>` - The value if any
- `description: &'static str` - A description of this configuration entry

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &ConfigEntry) -> bool`



## datafusion_common::config::ConfigExtension

*Trait*

[`ConfigExtension`] provides a mechanism to store third-party configuration
within DataFusion [`ConfigOptions`]

This mechanism can be used to pass configuration to user defined functions
or optimizer passes

# Example
```
use datafusion_common::{
    config::ConfigExtension, config::ConfigOptions, extensions_options,
};
// Define a new configuration struct using the `extensions_options` macro
extensions_options! {
   /// My own config options.
   pub struct MyConfig {
       /// Should "foo" be replaced by "bar"?
       pub foo_to_bar: bool, default = true

       /// How many "baz" should be created?
       pub baz_count: usize, default = 1337
   }
}

impl ConfigExtension for MyConfig {
    const PREFIX: &'static str = "my_config";
}

// set up config struct and register extension
let mut config = ConfigOptions::default();
config.extensions.insert(MyConfig::default());

// overwrite config default
config.set("my_config.baz_count", "42").unwrap();

// check config state
let my_config = config.extensions.get::<MyConfig>().unwrap();
assert!(my_config.foo_to_bar,);
assert_eq!(my_config.baz_count, 42,);
```

# Note:
Unfortunately associated constants are not currently object-safe, and so this
extends the object-safe [`ExtensionOptions`]

**Methods:**

- `PREFIX`: Configuration namespace prefix to use



## datafusion_common::config::ConfigField

*Trait*

A trait implemented by `config_namespace` and for field types that provides
the ability to walk and mutate the configuration tree

**Methods:**

- `visit`
- `set`
- `reset`



## datafusion_common::config::ConfigFileDecryptionProperties

*Struct*

**Fields:**
- `footer_key_as_hex: String` - Binary string to use for the parquet footer encoded in hex format
- `column_decryption_properties: std::collections::HashMap<String, ColumnDecryptionProperties>` - HashMap of column names --> key in hex format
- `aad_prefix_as_hex: String` - AAD prefix string uniquely identifies the file and prevents file swapping
- `footer_signature_verification: bool` - If true, then verify signature for files with plaintext footers.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ConfigFileDecryptionProperties) -> bool`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> ConfigFileDecryptionProperties`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`



## datafusion_common::config::ConfigFileEncryptionProperties

*Struct*

**Fields:**
- `encrypt_footer: bool` - Should the parquet footer be encrypted
- `footer_key_as_hex: String` - Key to use for the parquet footer encoded in hex format
- `footer_key_metadata_as_hex: String` - Metadata information for footer key
- `column_encryption_properties: std::collections::HashMap<String, ColumnEncryptionProperties>` - HashMap of column names --> (key in hex format, metadata)
- `aad_prefix_as_hex: String` - AAD prefix string uniquely identifies the file and prevents file swapping
- `store_aad_prefix: bool` - If true, store the AAD prefix in the file

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ConfigFileEncryptionProperties) -> bool`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> ConfigFileEncryptionProperties`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`



## datafusion_common::config::ConfigFileType

*Enum*

These file types have special built in behavior for configuration.
Use TableOptions::Extensions for configuring other file types.

**Variants:**
- `CSV`
- `PARQUET`
- `JSON`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ConfigFileType`



## datafusion_common::config::ConfigOptions

*Struct*

Configuration options struct, able to store both built-in configuration and custom options

**Fields:**
- `catalog: CatalogOptions` - Catalog options
- `execution: ExecutionOptions` - Execution options
- `optimizer: OptimizerOptions` - Optimizer options
- `sql_parser: SqlParserOptions` - SQL parser options
- `explain: ExplainOptions` - Explain options
- `extensions: Extensions` - Optional extensions registered using [`Extensions::insert`]
- `format: FormatOptions` - Formatting options when printing batches

**Methods:**

- `fn new() -> Self` - Creates a new [`ConfigOptions`] with default values
- `fn with_extensions(self: Self, extensions: Extensions) -> Self` - Set extensions to provided value
- `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>` - Set a configuration option
- `fn from_env() -> Result<Self>` - Create new [`ConfigOptions`], taking values from environment variables
- `fn from_string_hash_map(settings: &HashMap<String, String>) -> Result<Self>` - Create new ConfigOptions struct, taking values from a string hash map.
- `fn entries(self: &Self) -> Vec<ConfigEntry>` - Returns the [`ConfigEntry`] stored within this [`ConfigOptions`]
- `fn generate_config_markdown() -> String` - Generate documentation that can be included in the user guide

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ConfigOptions`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, _key_prefix: &str, _description: &'static str)`
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`
  - `fn reset(self: & mut Self, key: &str) -> Result<()>` - Reset a configuration option back to its default value
- **Default**
  - `fn default() -> ConfigOptions`



## datafusion_common::config::CsvOptions

*Struct*

Options controlling CSV format

**Fields:**
- `has_header: Option<bool>` - Specifies whether there is a CSV header (i.e. the first line
- `delimiter: u8`
- `quote: u8`
- `terminator: Option<u8>`
- `escape: Option<u8>`
- `double_quote: Option<bool>`
- `newlines_in_values: Option<bool>` - Specifies whether newlines in (quoted) values are supported.
- `compression: crate::parsers::CompressionTypeVariant`
- `compression_level: Option<u32>` - Compression level for the output file. The valid range depends on the
- `schema_infer_max_rec: Option<usize>`
- `date_format: Option<String>`
- `datetime_format: Option<String>`
- `timestamp_format: Option<String>`
- `timestamp_tz_format: Option<String>`
- `time_format: Option<String>`
- `null_value: Option<String>`
- `null_regex: Option<String>`
- `comment: Option<u8>`
- `truncated_rows: Option<bool>` - Whether to allow truncated rows when parsing, both within a single file and across files.

**Methods:**

- `fn with_compression(self: Self, compression_type_variant: CompressionTypeVariant) -> Self` - Set a limit in terms of records to scan to infer the schema
- `fn with_schema_infer_max_rec(self: Self, max_rec: usize) -> Self` - Set a limit in terms of records to scan to infer the schema
- `fn with_has_header(self: Self, has_header: bool) -> Self` - Set true to indicate that the first line is a header.
- `fn has_header(self: &Self) -> Option<bool>` - Returns true if the first line is a header. If format options does not
- `fn with_delimiter(self: Self, delimiter: u8) -> Self` - The character separating values within a row.
- `fn with_quote(self: Self, quote: u8) -> Self` - The quote character in a row.
- `fn with_terminator(self: Self, terminator: Option<u8>) -> Self` - The character that terminates a row.
- `fn with_escape(self: Self, escape: Option<u8>) -> Self` - The escape character in a row.
- `fn with_double_quote(self: Self, double_quote: bool) -> Self` - Set true to indicate that the CSV quotes should be doubled.
- `fn with_newlines_in_values(self: Self, newlines_in_values: bool) -> Self` - Specifies whether newlines in (quoted) values are supported.
- `fn with_file_compression_type(self: Self, compression: CompressionTypeVariant) -> Self` - Set a `CompressionTypeVariant` of CSV
- `fn with_truncated_rows(self: Self, allow: bool) -> Self` - Whether to allow truncated rows when parsing.
- `fn with_compression_level(self: Self, level: u32) -> Self` - Set the compression level for the output file.
- `fn delimiter(self: &Self) -> u8` - The delimiter character.
- `fn quote(self: &Self) -> u8` - The quote character.
- `fn terminator(self: &Self) -> Option<u8>` - The terminator character.
- `fn escape(self: &Self) -> Option<u8>` - The escape character.

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &CsvOptions) -> bool`
- **Default**
  - `fn default() -> Self`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CsvOptions`



## datafusion_common::config::Dialect

*Enum*

This is the SQL dialect used by DataFusion's parser.
This mirrors [sqlparser::dialect::Dialect](https://docs.rs/sqlparser/latest/sqlparser/dialect/trait.Dialect.html)
trait in order to offer an easier API and avoid adding the `sqlparser` dependency

**Variants:**
- `Generic`
- `MySQL`
- `PostgreSQL`
- `Hive`
- `SQLite`
- `Snowflake`
- `Redshift`
- `MsSQL`
- `ClickHouse`
- `BigQuery`
- `Ansi`
- `DuckDB`
- `Databricks`

**Traits:** Eq, Copy

**Trait Implementations:**

- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, description: &'static str)`
  - `fn set(self: & mut Self, _: &str, value: &str) -> Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> Dialect`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Default**
  - `fn default() -> Dialect`
- **PartialEq**
  - `fn eq(self: &Self, other: &Dialect) -> bool`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`



## datafusion_common::config::EncryptionFactoryOptions

*Struct*

Holds implementation-specific options for an encryption factory

**Fields:**
- `options: std::collections::HashMap<String, String>`

**Methods:**

- `fn to_extension_options<T>(self: &Self) -> Result<T>` - Convert these encryption factory options to an [`ExtensionOptions`] instance.

**Trait Implementations:**

- **Default**
  - `fn default() -> EncryptionFactoryOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &EncryptionFactoryOptions) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> EncryptionFactoryOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, _description: &'static str)`
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`



## datafusion_common::config::ExecutionOptions

*Struct*

Options related to query execution

See also: [`SessionConfig`]

[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html

**Fields:**
- `batch_size: usize` - Default batch size while creating new batches, it's especially useful for
- `coalesce_batches: bool` - When set to true, record batches will be examined between each operator and
- `collect_statistics: bool` - Should DataFusion collect statistics when first creating a table.
- `target_partitions: usize` - Number of partitions for query execution. Increasing partitions can increase
- `time_zone: Option<String>` - The default time zone
- `parquet: ParquetOptions` - Parquet options
- `planning_concurrency: usize` - Fan-out during initial physical planning.
- `skip_physical_aggregate_schema_check: bool` - When set to true, skips verifying that the schema produced by
- `spill_compression: SpillCompression` - Sets the compression codec used when spilling data to disk.
- `sort_spill_reservation_bytes: usize` - Specifies the reserved memory for each spillable sort operation to
- `sort_in_place_threshold_bytes: usize` - When sorting, below what size should data be concatenated
- `max_spill_file_size_bytes: usize` - Maximum size in bytes for individual spill files before rotating to a new file.
- `meta_fetch_concurrency: usize` - Number of files to read in parallel when inferring schema and statistics
- `minimum_parallel_output_files: usize` - Guarantees a minimum level of output files running in parallel.
- `soft_max_rows_per_output_file: usize` - Target number of rows in output files when writing multiple.
- `max_buffered_batches_per_output_file: usize` - This is the maximum number of RecordBatches buffered
- `listing_table_ignore_subdirectory: bool` - Should sub directories be ignored when scanning directories for data
- `listing_table_factory_infer_partitions: bool` - Should a `ListingTable` created through the `ListingTableFactory` infer table
- `enable_recursive_ctes: bool` - Should DataFusion support recursive CTEs
- `split_file_groups_by_statistics: bool` - Attempt to eliminate sorts by packing & sorting files with non-overlapping
- `keep_partition_by_columns: bool` - Should DataFusion keep the columns used for partition_by in the output RecordBatches
- `skip_partial_aggregation_probe_ratio_threshold: f64` - Aggregation ratio (number of distinct groups / number of input rows)
- `skip_partial_aggregation_probe_rows_threshold: usize` - Number of input rows partial aggregation partition should process, before
- `use_row_number_estimates_to_optimize_partitioning: bool` - Should DataFusion use row number estimates at the input to decide
- `enforce_batch_size_in_joins: bool` - Should DataFusion enforce batch size in joins or not. By default,
- `objectstore_writer_buffer_size: usize` - Size (bytes) of data buffer DataFusion uses when writing output files.
- `enable_ansi_mode: bool` - Whether to enable ANSI SQL mode.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> ExecutionOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExecutionOptions) -> bool`
- **Default**
  - `fn default() -> Self`



## datafusion_common::config::ExplainOptions

*Struct*

Options controlling explain output

See also: [`SessionConfig`]

[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html

**Fields:**
- `logical_plan_only: bool` - When set to true, the explain statement will only print logical plans
- `physical_plan_only: bool` - When set to true, the explain statement will only print physical plans
- `show_statistics: bool` - When set to true, the explain statement will print operator statistics
- `show_sizes: bool` - When set to true, the explain statement will print the partition sizes
- `show_schema: bool` - When set to true, the explain statement will print schema information
- `format: crate::format::ExplainFormat` - Display format of explain. Default is "indent".
- `tree_maximum_render_width: usize` - (format=tree only) Maximum total width of the rendered tree.
- `analyze_level: crate::format::ExplainAnalyzeLevel` - Verbosity level for "EXPLAIN ANALYZE". Default is "dev"

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ExplainOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExplainOptions) -> bool`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`



## datafusion_common::config::ExtensionOptions

*Trait*

An object-safe API for storing arbitrary configuration.

See [`ConfigExtension`] for user defined configuration

**Methods:**

- `as_any`: Return `self` as [`Any`]
- `as_any_mut`: Return `self` as [`Any`]
- `cloned`: Return a deep clone of this [`ExtensionOptions`]
- `set`: Set the given `key`, `value` pair
- `entries`: Returns the [`ConfigEntry`] stored in this [`ExtensionOptions`]



## datafusion_common::config::Extensions

*Struct*

A type-safe container for [`ConfigExtension`]

**Tuple Struct**: `()`

**Methods:**

- `fn new() -> Self` - Create a new, empty [`Extensions`]
- `fn insert<T>(self: & mut Self, extension: T)` - Registers a [`ConfigExtension`] with this [`ConfigOptions`]
- `fn get<T>(self: &Self) -> Option<&T>` - Retrieves the extension of the given type if any
- `fn get_mut<T>(self: & mut Self) -> Option<& mut T>` - Retrieves the extension of the given type if any
- `fn iter(self: &Self) -> impl Trait` - Iterates all the config extension entries yielding their prefix and their

**Trait Implementations:**

- **Default**
  - `fn default() -> Extensions`
- **Clone**
  - `fn clone(self: &Self) -> Extensions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::config::FormatOptions

*Struct*

Options controlling the format of output when printing record batches
Copies [`arrow::util::display::FormatOptions`]

**Fields:**
- `safe: bool` - If set to `true` any formatting errors will be written to the output
- `null: String` - Format string for nulls
- `date_format: Option<String>` - Date format for date arrays
- `datetime_format: Option<String>` - Format for DateTime arrays
- `timestamp_format: Option<String>` - Timestamp format for timestamp arrays
- `timestamp_tz_format: Option<String>` - Timestamp format for timestamp with timezone arrays. When `None`, ISO 8601 format is used.
- `time_format: Option<String>` - Time format for time arrays
- `duration_format: String` - Duration format. Can be either `"pretty"` or `"ISO8601"`
- `types_info: bool` - Show types in visual representation batches

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> FormatOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &FormatOptions) -> bool`



## datafusion_common::config::JsonOptions

*Struct*

Options controlling JSON format

**Fields:**
- `compression: crate::parsers::CompressionTypeVariant`
- `compression_level: Option<u32>` - Compression level for the output file. The valid range depends on the
- `schema_infer_max_rec: Option<usize>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> JsonOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &JsonOptions) -> bool`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`



## datafusion_common::config::OptimizerOptions

*Struct*

Options related to query optimization

See also: [`SessionConfig`]

[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html

**Fields:**
- `enable_distinct_aggregation_soft_limit: bool` - When set to true, the optimizer will push a limit operation into
- `enable_round_robin_repartition: bool` - When set to true, the physical plan optimizer will try to add round robin
- `enable_topk_aggregation: bool` - When set to true, the optimizer will attempt to perform limit operations
- `enable_window_limits: bool` - When set to true, the optimizer will attempt to push limit operations
- `enable_topk_dynamic_filter_pushdown: bool` - When set to true, the optimizer will attempt to push down TopK dynamic filters
- `enable_join_dynamic_filter_pushdown: bool` - When set to true, the optimizer will attempt to push down Join dynamic filters
- `enable_aggregate_dynamic_filter_pushdown: bool` - When set to true, the optimizer will attempt to push down Aggregate dynamic filters
- `enable_dynamic_filter_pushdown: bool` - When set to true attempts to push down dynamic filters generated by operators (TopK, Join & Aggregate) into the file scan phase.
- `filter_null_join_keys: bool` - When set to true, the optimizer will insert filters before a join between
- `repartition_aggregations: bool` - Should DataFusion repartition data using the aggregate keys to execute aggregates
- `repartition_file_min_size: usize` - Minimum total files size in bytes to perform file scan repartitioning.
- `repartition_joins: bool` - Should DataFusion repartition data using the join keys to execute joins in parallel
- `allow_symmetric_joins_without_pruning: bool` - Should DataFusion allow symmetric hash joins for unbounded data sources even when
- `repartition_file_scans: bool` - When set to `true`, datasource partitions will be repartitioned to achieve maximum parallelism.
- `preserve_file_partitions: usize` - Minimum number of distinct partition values required to group files by their
- `repartition_windows: bool` - Should DataFusion repartition data using the partitions keys to execute window
- `repartition_sorts: bool` - Should DataFusion execute sorts in a per-partition fashion and merge
- `subset_repartition_threshold: usize` - Partition count threshold for subset satisfaction optimization.
- `prefer_existing_sort: bool` - When true, DataFusion will opportunistically remove sorts when the data is already sorted,
- `skip_failed_rules: bool` - When set to true, the logical plan optimizer will produce warning
- `max_passes: usize` - Number of times that the optimizer will attempt to optimize the plan
- `top_down_join_key_reordering: bool` - When set to true, the physical plan optimizer will run a top down
- `prefer_hash_join: bool` - When set to true, the physical plan optimizer will prefer HashJoin over SortMergeJoin.
- `enable_piecewise_merge_join: bool` - When set to true, piecewise merge join is enabled. PiecewiseMergeJoin is currently
- `hash_join_single_partition_threshold: usize` - The maximum estimated size in bytes for one input side of a HashJoin
- `hash_join_single_partition_threshold_rows: usize` - The maximum estimated size in rows for one input side of a HashJoin
- `hash_join_inlist_pushdown_max_size: usize` - Maximum size in bytes for the build side of a hash join to be pushed down as an InList expression for dynamic filtering.
- `hash_join_inlist_pushdown_max_distinct_values: usize` - Maximum number of distinct values (rows) in the build side of a hash join to be pushed down as an InList expression for dynamic filtering.
- `default_filter_selectivity: u8` - The default filter selectivity used by Filter Statistics
- `prefer_existing_union: bool` - When set to true, the optimizer will not attempt to convert Union to Interleave
- `expand_views_at_output: bool` - When set to true, if the returned type is a view type
- `enable_sort_pushdown: bool` - Enable sort pushdown optimization.

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> OptimizerOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &OptimizerOptions) -> bool`



## datafusion_common::config::OutputFormat

*Enum*

**Variants:**
- `CSV(CsvOptions)`
- `JSON(JsonOptions)`
- `PARQUET(TableParquetOptions)`
- `AVRO`
- `ARROW`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> OutputFormat`
- **PartialEq**
  - `fn eq(self: &Self, other: &OutputFormat) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_common::config::OutputFormatExt

*Trait*



## datafusion_common::config::ParquetColumnOptions

*Struct*

Options controlling parquet format for individual columns.

See [`ParquetOptions`] for more details

**Fields:**
- `bloom_filter_enabled: Option<bool>` - Sets if bloom filter is enabled for the column path.
- `encoding: Option<String>` - Sets encoding for the column path.
- `dictionary_enabled: Option<bool>` - Sets if dictionary encoding is enabled for the column path. If NULL, uses
- `compression: Option<String>` - Sets default parquet compression codec for the column path.
- `statistics_enabled: Option<String>` - Sets if statistics are enabled for the column
- `bloom_filter_fpp: Option<f64>` - Sets bloom filter false positive probability for the column path. If NULL, uses
- `bloom_filter_ndv: Option<u64>` - Sets bloom filter number of distinct values. If NULL, uses

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ParquetColumnOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &ParquetColumnOptions) -> bool`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::config::ParquetEncryptionOptions

*Struct*

Options for configuring Parquet Modular Encryption

To use Parquet encryption, you must enable the `parquet_encryption` feature flag, as it is not activated by default.

**Fields:**
- `file_decryption: Option<ConfigFileDecryptionProperties>` - Optional file decryption properties
- `file_encryption: Option<ConfigFileEncryptionProperties>` - Optional file encryption properties
- `factory_id: Option<String>` - Identifier for the encryption factory to use to create file encryption and decryption properties.
- `factory_options: EncryptionFactoryOptions` - Any encryption factory specific options

**Methods:**

- `fn configure_factory<impl ExtensionOptions>(self: & mut Self, factory_id: &str, config: &impl Trait)` - Specify the encryption factory to use for Parquet modular encryption, along with its configuration

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> ParquetEncryptionOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &ParquetEncryptionOptions) -> bool`
- **Default**
  - `fn default() -> Self`



## datafusion_common::config::ParquetOptions

*Struct*

Options for reading and writing parquet files

See also: [`SessionConfig`]

[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html

**Fields:**
- `enable_page_index: bool` - (reading) If true, reads the Parquet data page level metadata (the
- `pruning: bool` - (reading) If true, the parquet reader attempts to skip entire row groups based
- `skip_metadata: bool` - (reading) If true, the parquet reader skip the optional embedded metadata that may be in
- `metadata_size_hint: Option<usize>` - (reading) If specified, the parquet reader will try and fetch the last `size_hint`
- `pushdown_filters: bool` - (reading) If true, filter expressions are be applied during the parquet decoding operation to
- `reorder_filters: bool` - (reading) If true, filter expressions evaluated during the parquet decoding operation
- `force_filter_selections: bool` - (reading) Force the use of RowSelections for filter results, when
- `schema_force_view_types: bool` - (reading) If true, parquet reader will read columns of `Utf8/Utf8Large` with `Utf8View`,
- `binary_as_string: bool` - (reading) If true, parquet reader will read columns of
- `coerce_int96: Option<String>` - (reading) If true, parquet reader will read columns of
- `bloom_filter_on_read: bool` - (reading) Use any available bloom filters when reading parquet files
- `max_predicate_cache_size: Option<usize>` - (reading) The maximum predicate cache size, in bytes. When
- `data_pagesize_limit: usize` - (writing) Sets best effort maximum size of data page in bytes
- `write_batch_size: usize` - (writing) Sets write_batch_size in bytes
- `writer_version: crate::parquet_config::DFParquetWriterVersion` - (writing) Sets parquet writer version
- `skip_arrow_metadata: bool` - (writing) Skip encoding the embedded arrow metadata in the KV_meta
- `compression: Option<String>` - (writing) Sets default parquet compression codec.
- `dictionary_enabled: Option<bool>` - (writing) Sets if dictionary encoding is enabled. If NULL, uses
- `dictionary_page_size_limit: usize` - (writing) Sets best effort maximum dictionary page size, in bytes
- `statistics_enabled: Option<String>` - (writing) Sets if statistics are enabled for any column
- `max_row_group_size: usize` - (writing) Target maximum number of rows in each row group (defaults to 1M
- `created_by: String` - (writing) Sets "created by" property
- `column_index_truncate_length: Option<usize>` - (writing) Sets column index truncate length
- `statistics_truncate_length: Option<usize>` - (writing) Sets statistics truncate length. If NULL, uses
- `data_page_row_count_limit: usize` - (writing) Sets best effort maximum number of rows in data page
- `encoding: Option<String>` - (writing)  Sets default encoding for any column.
- `bloom_filter_on_write: bool` - (writing) Write bloom filters for all columns when creating parquet files
- `bloom_filter_fpp: Option<f64>` - (writing) Sets bloom filter false positive probability. If NULL, uses
- `bloom_filter_ndv: Option<u64>` - (writing) Sets bloom filter number of distinct values. If NULL, uses
- `allow_single_file_parallelism: bool` - (writing) Controls whether DataFusion will attempt to speed up writing
- `maximum_parallel_row_group_writers: usize` - (writing) By default parallel parquet writer is tuned for minimum
- `maximum_buffered_record_batches_per_stream: usize` - (writing) By default parallel parquet writer is tuned for minimum

**Methods:**

- `fn into_writer_properties_builder(self: &Self) -> Result<WriterPropertiesBuilder>` - Convert the global session options, [`ParquetOptions`], into a single write action's [`WriterPropertiesBuilder`].

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`
- **Clone**
  - `fn clone(self: &Self) -> ParquetOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &ParquetOptions) -> bool`
- **Default**
  - `fn default() -> Self`



## datafusion_common::config::SpillCompression

*Enum*

**Variants:**
- `Zstd`
- `Lz4Frame`
- `Uncompressed`

**Traits:** Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SpillCompression`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key: &str, description: &'static str)`
  - `fn set(self: & mut Self, _: &str, value: &str) -> Result<()>`
- **Default**
  - `fn default() -> SpillCompression`
- **PartialEq**
  - `fn eq(self: &Self, other: &SpillCompression) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_common::config::SqlParserOptions

*Struct*

Options related to SQL parser

See also: [`SessionConfig`]

[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html

**Fields:**
- `parse_float_as_decimal: bool` - When set to true, SQL parser will parse float as decimal type
- `enable_ident_normalization: bool` - When set to true, SQL parser will normalize ident (convert ident to lowercase when not quoted)
- `enable_options_value_normalization: bool` - When set to true, SQL parser will normalize options value (convert value to lowercase).
- `dialect: Dialect` - Configure the SQL dialect used by DataFusion's parser; supported values include: Generic,
- `support_varchar_with_length: bool` - If true, permit lengths for `VARCHAR` such as `VARCHAR(20)`, but
- `map_string_types_to_utf8view: bool` - If true, string types (VARCHAR, CHAR, Text, and String) are mapped to `Utf8View` during SQL planning.
- `collect_spans: bool` - When set to true, the source locations relative to the original SQL
- `recursion_limit: usize` - Specifies the recursion depth limit when parsing complex SQL Queries
- `default_null_ordering: String` - Specifies the default null ordering for query results. There are 4 options:

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SqlParserOptions`
- **PartialEq**
  - `fn eq(self: &Self, other: &SqlParserOptions) -> bool`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn set(self: & mut Self, key: &str, value: &str) -> $crate::error::Result<()>`
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, _description: &'static str)`
  - `fn reset(self: & mut Self, key: &str) -> $crate::error::Result<()>`



## datafusion_common::config::TableOptions

*Struct*

Represents the configuration options available for handling different table formats within a data processing application.
This struct encompasses options for various file formats including CSV, Parquet, and JSON, allowing for flexible configuration
of parsing and writing behaviors specific to each format. Additionally, it supports extending functionality through custom extensions.

**Fields:**
- `csv: CsvOptions` - Configuration options for CSV file handling. This includes settings like the delimiter,
- `parquet: TableParquetOptions` - Configuration options for Parquet file handling. This includes settings for compression,
- `json: JsonOptions` - Configuration options for JSON file handling.
- `current_format: Option<ConfigFileType>` - The current file format that the table operations should assume. This option allows
- `extensions: Extensions` - Optional extensions that can be used to extend or customize the behavior of the table

**Methods:**

- `fn new() -> Self` - Constructs a new instance of `TableOptions` with default settings.
- `fn default_from_session_config(config: &ConfigOptions) -> Self` - Creates a new `TableOptions` instance initialized with settings from a given session config.
- `fn combine_with_session_config(self: &Self, config: &ConfigOptions) -> Self` - Updates the current `TableOptions` with settings from a given session config.
- `fn set_config_format(self: & mut Self, format: ConfigFileType)` - Sets the file format for the table.
- `fn with_extensions(self: Self, extensions: Extensions) -> Self` - Sets the extensions for this `TableOptions` instance.
- `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>` - Sets a specific configuration option.
- `fn from_string_hash_map(settings: &HashMap<String, String>) -> Result<Self>` - Initializes a new `TableOptions` from a hash map of string settings.
- `fn alter_with_string_hash_map(self: & mut Self, settings: &HashMap<String, String>) -> Result<()>` - Modifies the current `TableOptions` instance with settings from a hash map.
- `fn entries(self: &Self) -> Vec<ConfigEntry>` - Retrieves all configuration entries from this `TableOptions`.

**Trait Implementations:**

- **Default**
  - `fn default() -> TableOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, _key_prefix: &str, _description: &'static str)` - Visits configuration settings for the current file format, or all formats if none is selected.
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>` - Sets a configuration value for a specific key within `TableOptions`.
- **Clone**
  - `fn clone(self: &Self) -> TableOptions`



## datafusion_common::config::TableParquetOptions

*Struct*

Options that control how Parquet files are read, including global options
that apply to all columns and optional column-specific overrides

Closely tied to [`ParquetWriterOptions`](crate::file_options::parquet_writer::ParquetWriterOptions).
Properties not included in [`TableParquetOptions`] may not be configurable at the external API
(e.g. sorting_columns).

**Fields:**
- `global: ParquetOptions` - Global Parquet options that propagates to all columns.
- `column_specific_options: std::collections::HashMap<String, ParquetColumnOptions>` - Column specific options. Default usage is parquet.XX::column.
- `key_value_metadata: std::collections::HashMap<String, Option<String>>` - Additional file-level metadata to include. Inserted into the key_value_metadata
- `crypto: ParquetEncryptionOptions` - Options for configuring Parquet modular encryption

**Methods:**

- `fn new() -> Self` - Return new default TableParquetOptions
- `fn with_skip_arrow_metadata(self: Self, skip: bool) -> Self` - Set whether the encoding of the arrow metadata should occur
- `fn entries(self: &TableParquetOptions) -> Vec<ConfigEntry>` - Retrieves all configuration entries from this `TableParquetOptions`.
- `fn arrow_schema(self: & mut Self, schema: &Arc<Schema>)` - Add the arrow schema to the parquet kv_metadata.

**Trait Implementations:**

- **ConfigField**
  - `fn visit<V>(self: &Self, v: & mut V, key_prefix: &str, description: &'static str)`
  - `fn set(self: & mut Self, key: &str, value: &str) -> Result<()>`
- **Default**
  - `fn default() -> TableParquetOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &TableParquetOptions) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> TableParquetOptions`



## datafusion_common::config::Visit

*Trait*

An implementation trait used to recursively walk configuration

**Methods:**

- `some`
- `none`



## datafusion_common::config::default_config_transform

*Function*

Default transformation to parse a [`ConfigField`] for a string.

This uses [`FromStr`] to parse the data.

```rust
fn default_config_transform<T>(input: &str) -> crate::Result<T>
```



