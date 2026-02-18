**datafusion_datasource**

# Module: datafusion_datasource

## Contents

**Modules**

- [`decoder`](#decoder) - Module containing helper methods for the various file formats
- [`display`](#display)
- [`file`](#file) - Common behaviors that every file format needs to implement
- [`file_compression_type`](#file_compression_type) - File Compression type abstraction
- [`file_format`](#file_format) - Module containing helper methods for the various file formats
- [`file_groups`](#file_groups) - Logic for managing groups of [`PartitionedFile`]s in DataFusion
- [`file_scan_config`](#file_scan_config) - [`FileScanConfig`] to configure scanning of possibly partitioned
- [`file_sink_config`](#file_sink_config)
- [`file_stream`](#file_stream) - A generic stream over file format readers that can be used by
- [`memory`](#memory)
- [`projection`](#projection)
- [`schema_adapter`](#schema_adapter) - Deprecated: [`SchemaAdapter`] and [`SchemaAdapterFactory`] have been removed.
- [`sink`](#sink) - Execution plan for writing data to [`DataSink`]s
- [`source`](#source) - [`DataSource`] and [`DataSourceExec`]
- [`table_schema`](#table_schema) - Helper struct to manage table schemas with partition columns
- [`url`](#url)
- [`write`](#write) - Module containing helper methods/traits related to enabling

**Structs**

- [`FileRange`](#filerange) - Only scan a subset of Row Groups from the Parquet file whose data "midpoint"
- [`PartitionedFile`](#partitionedfile) - A single file or part of a file that should be read, along with its schema, statistics

**Enums**

- [`RangeCalculation`](#rangecalculation) - Represents the possible outcomes of a range calculation.

**Functions**

- [`calculate_range`](#calculate_range) - Calculates an appropriate byte range for reading from an object based on the
- [`generate_test_files`](#generate_test_files) - Generates test files with min-max statistics in different overlap patterns.
- [`verify_sort_integrity`](#verify_sort_integrity) - Used by tests and benchmarks

**Type Aliases**

- [`PartitionedFileStream`](#partitionedfilestream) - Stream of files get listed from object store

---

## datafusion_datasource::FileRange

*Struct*

Only scan a subset of Row Groups from the Parquet file whose data "midpoint"
lies within the [start, end) byte offsets. This option can be used to scan non-overlapping
sections of a Parquet file in parallel.

**Fields:**
- `start: i64` - Range start
- `end: i64` - Range end

**Methods:**

- `fn contains(self: &Self, offset: i64) -> bool` - returns true if this file range contains the specified offset

**Traits:** Eq

**Trait Implementations:**

- **Ord**
  - `fn cmp(self: &Self, other: &FileRange) -> $crate::cmp::Ordering`
- **PartialEq**
  - `fn eq(self: &Self, other: &FileRange) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &FileRange) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> FileRange`



## datafusion_datasource::PartitionedFile

*Struct*

A single file or part of a file that should be read, along with its schema, statistics
and partition column values that need to be appended to each row.

# Statistics

The [`Self::statistics`] field contains statistics for the **full table schema**,
which includes both file columns and partition columns. When statistics are set via
[`Self::with_statistics`], exact statistics for partition columns are automatically
computed from [`Self::partition_values`]:

- `min = max = partition_value` (all rows in a file share the same partition value)
- `null_count = 0` (partition values extracted from paths are never null)
- `distinct_count = 1` (single distinct value per file for each partition column)

This enables query optimizers to use partition column bounds for pruning and planning.

**Fields:**
- `object_meta: object_store::ObjectMeta` - Path for the file (e.g. URL, filesystem path, etc)
- `partition_values: Vec<datafusion_common::ScalarValue>` - Values of partition columns to be appended to each row.
- `range: Option<FileRange>` - An optional file range for a more fine-grained parallel execution
- `statistics: Option<std::sync::Arc<datafusion_common::Statistics>>` - Optional statistics that describe the data in this file if known.
- `extensions: Option<std::sync::Arc<dyn std::any::Any>>` - An optional field for user defined per object metadata
- `metadata_size_hint: Option<usize>` - The estimated size of the parquet metadata, in bytes

**Methods:**

- `fn new<impl Into<String>>(path: impl Trait, size: u64) -> Self` - Create a simple file without metadata or partition
- `fn new_with_range(path: String, size: u64, start: i64, end: i64) -> Self` - Create a file range without metadata or partition
- `fn effective_size(self: &Self) -> u64` - Size of the file to be scanned (taking into account the range, if present).
- `fn range(self: &Self) -> (u64, u64)` - Effective range of the file to be scanned.
- `fn with_metadata_size_hint(self: Self, metadata_size_hint: usize) -> Self` - Provide a hint to the size of the file metadata. If a hint is provided
- `fn from_path(path: String) -> Result<Self>` - Return a file reference from the given path
- `fn path(self: &Self) -> &Path` - Return the path of this partitioned file
- `fn with_range(self: Self, start: i64, end: i64) -> Self` - Update the file to only scan the specified range (in bytes)
- `fn with_extensions(self: Self, extensions: Arc<dyn std::any::Any>) -> Self` - Update the user defined extensions for this file.
- `fn with_statistics(self: Self, file_statistics: Arc<Statistics>) -> Self` - Update the statistics for this file.
- `fn has_statistics(self: &Self) -> bool` - Check if this file has any statistics.

**Trait Implementations:**

- **From**
  - `fn from(object_meta: ObjectMeta) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PartitionedFile`



## datafusion_datasource::PartitionedFileStream

*Type Alias*: `std::pin::Pin<Box<dyn Stream>>`

Stream of files get listed from object store



## datafusion_datasource::RangeCalculation

*Enum*

Represents the possible outcomes of a range calculation.

This enum is used to encapsulate the result of calculating the range of
bytes to read from an object (like a file) in an object store.

Variants:
- `Range(Option<Range<usize>>)`:
  Represents a range of bytes to be read. It contains an `Option` wrapping a
  `Range<usize>`. `None` signifies that the entire object should be read,
  while `Some(range)` specifies the exact byte range to read.
- `TerminateEarly`:
  Indicates that the range calculation determined no further action is
  necessary, possibly because the calculated range is empty or invalid.

**Variants:**
- `Range(Option<std::ops::Range<u64>>)`
- `TerminateEarly`



## datafusion_datasource::calculate_range

*Function*

Calculates an appropriate byte range for reading from an object based on the
provided metadata.

This asynchronous function examines the [`PartitionedFile`] of an object in an object store
and determines the range of bytes to be read. The range calculation may adjust
the start and end points to align with meaningful data boundaries (like newlines).

Returns a `Result` wrapping a [`RangeCalculation`], which is either a calculated byte range or an indication to terminate early.

Returns an `Error` if any part of the range calculation fails, such as issues in reading from the object store or invalid range boundaries.

```rust
fn calculate_range(file: &PartitionedFile, store: &std::sync::Arc<dyn ObjectStore>, terminator: Option<u8>) -> datafusion_common::Result<RangeCalculation>
```



## Module: decoder

Module containing helper methods for the various file formats
See write.rs for write related helper methods



## Module: display



## Module: file

Common behaviors that every file format needs to implement



## Module: file_compression_type

File Compression type abstraction



## Module: file_format

Module containing helper methods for the various file formats
See write.rs for write related helper methods



## Module: file_groups

Logic for managing groups of [`PartitionedFile`]s in DataFusion



## Module: file_scan_config

[`FileScanConfig`] to configure scanning of possibly partitioned
file sources.



## Module: file_sink_config



## Module: file_stream

A generic stream over file format readers that can be used by
any file format that read its files from start to end.

Note: Most traits here need to be marked `Sync + Send` to be
compliant with the `SendableRecordBatchStream` trait.



## datafusion_datasource::generate_test_files

*Function*

Generates test files with min-max statistics in different overlap patterns.

Used by tests and benchmarks.

# Overlap Factors

The `overlap_factor` parameter controls how much the value ranges in generated test files overlap:
- `0.0`: No overlap between files (completely disjoint ranges)
- `0.2`: Low overlap (20% of the range size overlaps with adjacent files)
- `0.5`: Medium overlap (50% of ranges overlap)
- `0.8`: High overlap (80% of ranges overlap between files)

# Examples

With 5 files and different overlap factors showing `[min, max]` ranges:

overlap_factor = 0.0 (no overlap):

File 0: [0, 20]
File 1: [20, 40]
File 2: [40, 60]
File 3: [60, 80]
File 4: [80, 100]

overlap_factor = 0.5 (50% overlap):

File 0: [0, 40]
File 1: [20, 60]
File 2: [40, 80]
File 3: [60, 100]
File 4: [80, 120]

overlap_factor = 0.8 (80% overlap):

File 0: [0, 100]
File 1: [20, 120]
File 2: [40, 140]
File 3: [60, 160]
File 4: [80, 180]

```rust
fn generate_test_files(num_files: usize, overlap_factor: f64) -> Vec<crate::file_groups::FileGroup>
```



## Module: memory



## Module: projection



## Module: schema_adapter

Deprecated: [`SchemaAdapter`] and [`SchemaAdapterFactory`] have been removed.

Use [`PhysicalExprAdapterFactory`] instead. See `upgrading.md` for more details.

[`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory



## Module: sink

Execution plan for writing data to [`DataSink`]s



## Module: source

[`DataSource`] and [`DataSourceExec`]



## Module: table_schema

Helper struct to manage table schemas with partition columns



## Module: url



## datafusion_datasource::verify_sort_integrity

*Function*

Used by tests and benchmarks

```rust
fn verify_sort_integrity(file_groups: &[crate::file_groups::FileGroup]) -> bool
```



## Module: write

Module containing helper methods/traits related to enabling
write support for the various file formats



