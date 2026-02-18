**datafusion_functions_aggregate > average**

# Module: average

## Contents

**Structs**

- [`Avg`](#avg)
- [`AvgAccumulator`](#avgaccumulator) - An accumulator to compute the average

**Functions**

- [`avg`](#avg) - Returns the avg of a group of values.
- [`avg_distinct`](#avg_distinct)
- [`avg_udaf`](#avg_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Avg`]

---

## datafusion_functions_aggregate::average::Avg

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Avg) -> bool`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::average::AvgAccumulator

*Struct*

An accumulator to compute the average

**Trait Implementations:**

- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn supports_retract_batch(self: &Self) -> bool`
- **Default**
  - `fn default() -> AvgAccumulator`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::average::avg

*Function*

Returns the avg of a group of values.

```rust
fn avg(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::average::avg_distinct

*Function*

```rust
fn avg_distinct(expr: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::average::avg_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Avg`]

```rust
fn avg_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



