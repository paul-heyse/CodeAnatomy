**datafusion_functions_aggregate > sum**

# Module: sum

## Contents

**Structs**

- [`SlidingDistinctSumAccumulator`](#slidingdistinctsumaccumulator) - A sliding‐window accumulator for `SUM(DISTINCT)` over Int64 columns.
- [`Sum`](#sum)

**Functions**

- [`sum`](#sum) - Returns the sum of a group of values.
- [`sum_distinct`](#sum_distinct)
- [`sum_udaf`](#sum_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Sum`]

---

## datafusion_functions_aggregate::sum::SlidingDistinctSumAccumulator

*Struct*

A sliding‐window accumulator for `SUM(DISTINCT)` over Int64 columns.
Maintains a running sum so that `evaluate()` is O(1).

**Methods:**

- `fn try_new(data_type: &DataType) -> Result<Self>` - Create a new accumulator; only `DataType::Int64` is supported.

**Trait Implementations:**

- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn supports_retract_batch(self: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::sum::Sum

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Sum) -> bool`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn create_sliding_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn set_monotonicity(self: &Self, data_type: &DataType) -> SetMonotonicity`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_aggregate::sum::sum

*Function*

Returns the sum of a group of values.

```rust
fn sum(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::sum::sum_distinct

*Function*

```rust
fn sum_distinct(expr: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::sum::sum_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Sum`]

```rust
fn sum_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



