**datafusion > test_util**

# Module: test_util

## Contents

**Modules**

- [`csv`](#csv) - Helpers for writing csv files and reading them back
- [`parquet`](#parquet) - Helpers for writing parquet files and reading them back

**Structs**

- [`TestTableFactory`](#testtablefactory) - TableFactory for tests
- [`TestTableProvider`](#testtableprovider) - TableProvider for testing purposes

**Functions**

- [`aggr_test_schema`](#aggr_test_schema) - Get the schema for the aggregate_test_* csv files
- [`bounded_stream`](#bounded_stream) - Creates a bounded stream that emits the same record batch a specified number of times.
- [`plan_and_collect`](#plan_and_collect) - Execute SQL and return results
- [`populate_csv_partitions`](#populate_csv_partitions) - Generate CSV partitions within the supplied directory
- [`register_aggregate_csv`](#register_aggregate_csv) - Register session context for the aggregate_test_100.csv file
- [`register_unbounded_file_with_ordering`](#register_unbounded_file_with_ordering) - This function creates an unbounded sorted file for testing purposes.
- [`scan_empty`](#scan_empty) - Scan an empty data source, mainly used in tests
- [`scan_empty_with_partitions`](#scan_empty_with_partitions) - Scan an empty data source with configured partition, mainly used in tests.
- [`test_table`](#test_table) - Create a table from the aggregate_test_100.csv file with the name "aggregate_test_100"
- [`test_table_with_cache_factory`](#test_table_with_cache_factory) - Create a test table registered to a session context with an associated cache factory
- [`test_table_with_name`](#test_table_with_name) - Create a table from the aggregate_test_100.csv file with the specified name

---

## datafusion::test_util::TestTableFactory

*Struct*

TableFactory for tests

**Trait Implementations:**

- **TableProviderFactory**
  - `fn create(self: &'life0 Self, _: &'life1 dyn Session, cmd: &'life2 CreateExternalTable) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Default**
  - `fn default() -> TestTableFactory`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion::test_util::TestTableProvider

*Struct*

TableProvider for testing purposes

**Fields:**
- `url: String` - URL of table files or folder
- `schema: arrow::datatypes::SchemaRef` - test table schema

**Trait Implementations:**

- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn table_type(self: &Self) -> TableType`
  - `fn scan(self: &'life0 Self, _state: &'life1 dyn Session, _projection: Option<&'life2 Vec<usize>>, _filters: &'life3 [Expr], _limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion::test_util::aggr_test_schema

*Function*

Get the schema for the aggregate_test_* csv files

```rust
fn aggr_test_schema() -> arrow::datatypes::SchemaRef
```



## datafusion::test_util::bounded_stream

*Function*

Creates a bounded stream that emits the same record batch a specified number of times.
This is useful for testing purposes.

```rust
fn bounded_stream(record_batch: arrow::record_batch::RecordBatch, limit: usize) -> crate::execution::SendableRecordBatchStream
```



## Module: csv

Helpers for writing csv files and reading them back



## Module: parquet

Helpers for writing parquet files and reading them back



## datafusion::test_util::plan_and_collect

*Function*

Execute SQL and return results

```rust
fn plan_and_collect(ctx: &crate::prelude::SessionContext, sql: &str) -> crate::error::Result<Vec<arrow::record_batch::RecordBatch>>
```



## datafusion::test_util::populate_csv_partitions

*Function*

Generate CSV partitions within the supplied directory

```rust
fn populate_csv_partitions(tmp_dir: &tempfile::TempDir, partition_count: usize, file_extension: &str) -> crate::error::Result<arrow::datatypes::SchemaRef>
```



## datafusion::test_util::register_aggregate_csv

*Function*

Register session context for the aggregate_test_100.csv file

```rust
fn register_aggregate_csv(ctx: &crate::prelude::SessionContext, table_name: &str) -> crate::error::Result<()>
```



## datafusion::test_util::register_unbounded_file_with_ordering

*Function*

This function creates an unbounded sorted file for testing purposes.

```rust
fn register_unbounded_file_with_ordering(ctx: &crate::prelude::SessionContext, schema: arrow::datatypes::SchemaRef, file_path: &std::path::Path, table_name: &str, file_sort_order: Vec<Vec<datafusion_expr::SortExpr>>) -> crate::error::Result<()>
```



## datafusion::test_util::scan_empty

*Function*

Scan an empty data source, mainly used in tests

```rust
fn scan_empty(name: Option<&str>, table_schema: &arrow::datatypes::Schema, projection: Option<Vec<usize>>) -> crate::error::Result<crate::logical_expr::LogicalPlanBuilder>
```



## datafusion::test_util::scan_empty_with_partitions

*Function*

Scan an empty data source with configured partition, mainly used in tests.

```rust
fn scan_empty_with_partitions(name: Option<&str>, table_schema: &arrow::datatypes::Schema, projection: Option<Vec<usize>>, partitions: usize) -> crate::error::Result<crate::logical_expr::LogicalPlanBuilder>
```



## datafusion::test_util::test_table

*Function*

Create a table from the aggregate_test_100.csv file with the name "aggregate_test_100"

```rust
fn test_table() -> crate::error::Result<crate::dataframe::DataFrame>
```



## datafusion::test_util::test_table_with_cache_factory

*Function*

Create a test table registered to a session context with an associated cache factory

```rust
fn test_table_with_cache_factory() -> crate::error::Result<crate::dataframe::DataFrame>
```



## datafusion::test_util::test_table_with_name

*Function*

Create a table from the aggregate_test_100.csv file with the specified name

```rust
fn test_table_with_name(name: &str) -> crate::error::Result<crate::dataframe::DataFrame>
```



