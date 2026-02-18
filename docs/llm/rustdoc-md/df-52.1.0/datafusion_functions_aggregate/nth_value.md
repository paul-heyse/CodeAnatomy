**datafusion_functions_aggregate > nth_value**

# Module: nth_value

## Contents

**Structs**

- [`NthValueAccumulator`](#nthvalueaccumulator)
- [`NthValueAgg`](#nthvalueagg) - Expression for a `NTH_VALUE(..., ... ORDER BY ...)` aggregation. In a multi
- [`TrivialNthValueAccumulator`](#trivialnthvalueaccumulator)

**Functions**

- [`nth_value`](#nth_value) - Returns the nth value in a group of values.
- [`nth_value_udaf`](#nth_value_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`NthValueAgg`]

---

## datafusion_functions_aggregate::nth_value::NthValueAccumulator

*Struct*

**Methods:**

- `fn try_new(n: i64, datatype: &DataType, ordering_dtypes: &[DataType], ordering_req: LexOrdering) -> Result<Self>` - Create a new order-sensitive NTH_VALUE accumulator based on the given

**Trait Implementations:**

- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>` - Updates its state with the `values`. Assumes data in the `values` satisfies the required
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::nth_value::NthValueAgg

*Struct*

Expression for a `NTH_VALUE(..., ... ORDER BY ...)` aggregation. In a multi
partition setting, partial aggregations are computed for every partition,
and then their results are merged.

**Methods:**

- `fn new() -> Self` - Create a new `NthValueAgg` aggregate function

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &NthValueAgg) -> bool`



## datafusion_functions_aggregate::nth_value::TrivialNthValueAccumulator

*Struct*

**Methods:**

- `fn try_new(n: i64, datatype: &DataType) -> Result<Self>` - Create a new order-insensitive NTH_VALUE accumulator based on the given

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>` - Updates its state with the `values`. Assumes data in the `values` satisfies the required
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::nth_value::nth_value

*Function*

Returns the nth value in a group of values.

```rust
fn nth_value(expr: datafusion_expr::Expr, n: i64, order_by: Vec<datafusion_expr::SortExpr>) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::nth_value::nth_value_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`NthValueAgg`]

```rust
fn nth_value_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



