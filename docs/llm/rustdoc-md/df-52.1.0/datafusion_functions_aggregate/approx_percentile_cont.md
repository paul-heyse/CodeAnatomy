**datafusion_functions_aggregate > approx_percentile_cont**

# Module: approx_percentile_cont

## Contents

**Structs**

- [`ApproxPercentileAccumulator`](#approxpercentileaccumulator)
- [`ApproxPercentileCont`](#approxpercentilecont)

**Functions**

- [`approx_percentile_cont`](#approx_percentile_cont) - Computes the approximate percentile continuous of a set of numbers
- [`approx_percentile_cont_udaf`](#approx_percentile_cont_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxPercentileCont`]

---

## datafusion_functions_aggregate::approx_percentile_cont::ApproxPercentileAccumulator

*Struct*

**Methods:**

- `fn new(percentile: f64, return_type: DataType) -> Self`
- `fn new_with_max_size(percentile: f64, return_type: DataType, max_size: usize) -> Self`

**Trait Implementations:**

- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::approx_percentile_cont::ApproxPercentileCont

*Struct*

**Methods:**

- `fn new() -> Self` - Create a new [`ApproxPercentileCont`] aggregate function.

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ApproxPercentileCont) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>` - See [`TDigest::to_scalar_state()`] for a description of the serialized
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn supports_within_group_clause(self: &Self) -> bool`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont

*Function*

Computes the approximate percentile continuous of a set of numbers

```rust
fn approx_percentile_cont(order_by: datafusion_expr::expr::Sort, percentile: datafusion_expr::Expr, centroids: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxPercentileCont`]

```rust
fn approx_percentile_cont_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



