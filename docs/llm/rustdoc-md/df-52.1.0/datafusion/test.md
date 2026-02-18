**datafusion > test**

# Module: test

## Contents

**Modules**

- [`object_store`](#object_store) - Object store implementation used for testing
- [`variable`](#variable) - System variable provider

**Functions**

- [`assert_fields_eq`](#assert_fields_eq)
- [`columns`](#columns) - Returns the column names on the schema
- [`create_table_dual`](#create_table_dual)
- [`make_partition`](#make_partition) - Return a RecordBatch with a single Int32 array with values (0..sz)
- [`partitioned_file_groups`](#partitioned_file_groups) - Returns file groups [`Vec<FileGroup>`] for scanning `partitions` of `filename`
- [`scan_partitioned_csv`](#scan_partitioned_csv) - Returns a [`DataSourceExec`] that scans "aggregate_test_100.csv" with `partitions` partitions
- [`table_with_decimal`](#table_with_decimal) - Return a new table which provide this decimal column
- [`table_with_sequence`](#table_with_sequence) - Return a new table provider that has a single Int32 column with

---

## datafusion::test::assert_fields_eq

*Function*

```rust
fn assert_fields_eq(plan: &crate::logical_expr::LogicalPlan, expected: &[&str])
```



## datafusion::test::columns

*Function*

Returns the column names on the schema

```rust
fn columns(schema: &arrow::datatypes::Schema) -> Vec<String>
```



## datafusion::test::create_table_dual

*Function*

```rust
fn create_table_dual() -> std::sync::Arc<dyn TableProvider>
```



## datafusion::test::make_partition

*Function*

Return a RecordBatch with a single Int32 array with values (0..sz)

```rust
fn make_partition(sz: i32) -> arrow::record_batch::RecordBatch
```



## Module: object_store

Object store implementation used for testing



## datafusion::test::partitioned_file_groups

*Function*

Returns file groups [`Vec<FileGroup>`] for scanning `partitions` of `filename`

```rust
fn partitioned_file_groups(path: &str, filename: &str, partitions: usize, file_format: &std::sync::Arc<dyn FileFormat>, file_compression_type: crate::datasource::file_format::file_compression_type::FileCompressionType, work_dir: &std::path::Path) -> crate::error::Result<Vec<datafusion_datasource::file_groups::FileGroup>>
```



## datafusion::test::scan_partitioned_csv

*Function*

Returns a [`DataSourceExec`] that scans "aggregate_test_100.csv" with `partitions` partitions

```rust
fn scan_partitioned_csv(partitions: usize, work_dir: &std::path::Path) -> crate::error::Result<std::sync::Arc<datafusion_datasource::source::DataSourceExec>>
```



## datafusion::test::table_with_decimal

*Function*

Return a new table which provide this decimal column

```rust
fn table_with_decimal() -> std::sync::Arc<dyn TableProvider>
```



## datafusion::test::table_with_sequence

*Function*

Return a new table provider that has a single Int32 column with
values between `seq_start` and `seq_end`

```rust
fn table_with_sequence(seq_start: i32, seq_end: i32) -> crate::error::Result<std::sync::Arc<dyn TableProvider>>
```



## Module: variable

System variable provider



