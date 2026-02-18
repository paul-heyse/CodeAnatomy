**datafusion_datasource_parquet > row_filter**

# Module: row_filter

## Contents

**Functions**

- [`build_row_filter`](#build_row_filter) - Build a [`RowFilter`] from the given predicate expression if possible.
- [`can_expr_be_pushed_down_with_schemas`](#can_expr_be_pushed_down_with_schemas) - Checks if a predicate expression can be pushed down to the parquet decoder.

---

## datafusion_datasource_parquet::row_filter::build_row_filter

*Function*

Build a [`RowFilter`] from the given predicate expression if possible.

# Arguments
* `expr` - The filter predicate, already adapted to reference columns in `file_schema`
* `file_schema` - The Arrow schema of the parquet file (the result of converting
  the parquet schema to Arrow, potentially with type coercions applied)
* `metadata` - Parquet file metadata used for cost estimation
* `reorder_predicates` - If true, reorder predicates to minimize I/O
* `file_metrics` - Metrics for tracking filter performance

# Returns
* `Ok(Some(row_filter))` if the expression can be used as a RowFilter
* `Ok(None)` if the expression cannot be used as a RowFilter
* `Err(e)` if an error occurs while building the filter

Note: The returned `RowFilter` may not contain all conjuncts from the original
expression. Conjuncts that cannot be evaluated as an `ArrowPredicate` are ignored.

For example, if the expression is `a = 1 AND b = 2 AND c = 3` and `b = 2`
cannot be evaluated for some reason, the returned `RowFilter` will contain
only `a = 1` and `c = 3`.

```rust
fn build_row_filter(expr: &std::sync::Arc<dyn PhysicalExpr>, file_schema: &arrow::datatypes::SchemaRef, metadata: &parquet::file::metadata::ParquetMetaData, reorder_predicates: bool, file_metrics: &super::ParquetFileMetrics) -> datafusion_common::Result<Option<parquet::arrow::arrow_reader::RowFilter>>
```



## datafusion_datasource_parquet::row_filter::can_expr_be_pushed_down_with_schemas

*Function*

Checks if a predicate expression can be pushed down to the parquet decoder.

Returns `true` if all columns referenced by the expression:
- Exist in the provided schema
- Are primitive types (not structs, lists, etc.)

# Arguments
* `expr` - The filter expression to check
* `file_schema` - The Arrow schema of the parquet file (or table schema when
  the file schema is not yet available during planning)

```rust
fn can_expr_be_pushed_down_with_schemas(expr: &std::sync::Arc<dyn PhysicalExpr>, file_schema: &arrow::datatypes::Schema) -> bool
```



