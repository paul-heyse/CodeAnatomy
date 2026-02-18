**datafusion_functions_aggregate > count**

# Module: count

## Contents

**Structs**

- [`Count`](#count)
- [`SlidingDistinctCountAccumulator`](#slidingdistinctcountaccumulator)

**Functions**

- [`count`](#count) - Count the number of non-null values in the column
- [`count_all`](#count_all) - Creates aggregation to count all rows.
- [`count_all_window`](#count_all_window) - Creates window aggregation to count all rows.
- [`count_distinct`](#count_distinct)
- [`count_udaf`](#count_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Count`]

---

## datafusion_functions_aggregate::count::Count

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn is_nullable(self: &Self) -> bool`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn default_value(self: &Self, _data_type: &DataType) -> Result<ScalarValue>`
  - `fn value_from_stats(self: &Self, statistics_args: &StatisticsArgs) -> Option<ScalarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn set_monotonicity(self: &Self, _data_type: &DataType) -> SetMonotonicity`
  - `fn create_sliding_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Count) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::count::SlidingDistinctCountAccumulator

*Struct*

**Methods:**

- `fn try_new(data_type: &DataType) -> Result<Self>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn supports_retract_batch(self: &Self) -> bool`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::count::count

*Function*

Count the number of non-null values in the column

```rust
fn count(expr: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::count::count_all

*Function*

Creates aggregation to count all rows.

In SQL this is `SELECT COUNT(*) ... `

The expression is equivalent to `COUNT(*)`, `COUNT()`, `COUNT(1)`, and is
aliased to a column named `"count(*)"` for backward compatibility.

Example
```
# use datafusion_functions_aggregate::count::count_all;
# use datafusion_expr::col;
// create `count(*)` expression
let expr = count_all();
assert_eq!(expr.schema_name().to_string(), "count(*)");
// if you need to refer to this column, use the `schema_name` function
let expr = col(expr.schema_name().to_string());
```

```rust
fn count_all() -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::count::count_all_window

*Function*

Creates window aggregation to count all rows.

In SQL this is `SELECT COUNT(*) OVER (..) ... `

The expression is equivalent to `COUNT(*)`, `COUNT()`, `COUNT(1)`

Example
```
# use datafusion_functions_aggregate::count::count_all_window;
# use datafusion_expr::col;
// create `count(*)` OVER ... window function expression
let expr = count_all_window();
assert_eq!(
    expr.schema_name().to_string(),
    "count(Int64(1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
);
// if you need to refer to this column, use the `schema_name` function
let expr = col(expr.schema_name().to_string());
```

```rust
fn count_all_window() -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::count::count_distinct

*Function*

```rust
fn count_distinct(expr: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::count::count_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Count`]

```rust
fn count_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



