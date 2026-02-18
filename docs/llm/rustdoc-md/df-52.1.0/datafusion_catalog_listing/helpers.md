**datafusion_catalog_listing > helpers**

# Module: helpers

## Contents

**Structs**

- [`Partition`](#partition)

**Functions**

- [`describe_partition`](#describe_partition) - Describe a partition as a (path, depth, files) tuple for easier assertions
- [`evaluate_partition_prefix`](#evaluate_partition_prefix)
- [`expr_applicable_for_cols`](#expr_applicable_for_cols) - Check whether the given expression can be resolved using only the columns `col_names`.
- [`list_partitions`](#list_partitions) - Returns a recursive list of the partitions in `table_path` up to `max_depth`
- [`parse_partitions_for_path`](#parse_partitions_for_path) - Extract the partition values for the given `file_path` (in the given `table_path`)
- [`pruned_partition_list`](#pruned_partition_list) - Discover the partitions on the given path and prune out files
- [`split_files`](#split_files) - Partition the list of files into `n` groups

---

## datafusion_catalog_listing::helpers::Partition

*Struct*

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_catalog_listing::helpers::describe_partition

*Function*

Describe a partition as a (path, depth, files) tuple for easier assertions

```rust
fn describe_partition(partition: &Partition) -> (&str, usize, Vec<&str>)
```



## datafusion_catalog_listing::helpers::evaluate_partition_prefix

*Function*

```rust
fn evaluate_partition_prefix<'a>(partition_cols: &'a [(String, arrow::datatypes::DataType)], filters: &'a [datafusion_expr::Expr]) -> Option<object_store::path::Path>
```



## datafusion_catalog_listing::helpers::expr_applicable_for_cols

*Function*

Check whether the given expression can be resolved using only the columns `col_names`.
This means that if this function returns true:
- the table provider can filter the table partition values with this expression
- the expression can be marked as `TableProviderFilterPushDown::Exact` once this filtering
  was performed

```rust
fn expr_applicable_for_cols(col_names: &[&str], expr: &datafusion_expr::Expr) -> bool
```



## datafusion_catalog_listing::helpers::list_partitions

*Function*

Returns a recursive list of the partitions in `table_path` up to `max_depth`

```rust
fn list_partitions(store: &dyn ObjectStore, table_path: &datafusion_datasource::ListingTableUrl, max_depth: usize, partition_prefix: Option<object_store::path::Path>) -> datafusion_common::Result<Vec<Partition>>
```



## datafusion_catalog_listing::helpers::parse_partitions_for_path

*Function*

Extract the partition values for the given `file_path` (in the given `table_path`)
associated to the partitions defined by `table_partition_cols`

```rust
fn parse_partitions_for_path<'a, I>(table_path: &datafusion_datasource::ListingTableUrl, file_path: &'a object_store::path::Path, table_partition_cols: I) -> Option<Vec<&'a str>>
```



## datafusion_catalog_listing::helpers::pruned_partition_list

*Function*

Discover the partitions on the given path and prune out files
that belong to irrelevant partitions using `filters` expressions.
`filters` should only contain expressions that can be evaluated
using only the partition columns.

```rust
fn pruned_partition_list<'a>(ctx: &'a dyn Session, store: &'a dyn ObjectStore, table_path: &'a datafusion_datasource::ListingTableUrl, filters: &'a [datafusion_expr::Expr], file_extension: &'a str, partition_cols: &'a [(String, arrow::datatypes::DataType)]) -> datafusion_common::Result<futures::stream::BoxStream<'a, datafusion_common::Result<datafusion_datasource::PartitionedFile>>>
```



## datafusion_catalog_listing::helpers::split_files

*Function*

Partition the list of files into `n` groups

```rust
fn split_files(partitioned_files: Vec<datafusion_datasource::PartitionedFile>, n: usize) -> Vec<Vec<datafusion_datasource::PartitionedFile>>
```



