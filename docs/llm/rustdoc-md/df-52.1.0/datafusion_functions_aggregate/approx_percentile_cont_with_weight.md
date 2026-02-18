**datafusion_functions_aggregate > approx_percentile_cont_with_weight**

# Module: approx_percentile_cont_with_weight

## Contents

**Structs**

- [`ApproxPercentileContWithWeight`](#approxpercentilecontwithweight) - APPROX_PERCENTILE_CONT_WITH_WEIGHT aggregate expression
- [`ApproxPercentileWithWeightAccumulator`](#approxpercentilewithweightaccumulator)

**Functions**

- [`approx_percentile_cont_with_weight`](#approx_percentile_cont_with_weight) - Computes the approximate percentile continuous with weight of a set of numbers
- [`approx_percentile_cont_with_weight_udaf`](#approx_percentile_cont_with_weight_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxPercentileContWithWeight`]

---

## datafusion_functions_aggregate::approx_percentile_cont_with_weight::ApproxPercentileContWithWeight

*Struct*

APPROX_PERCENTILE_CONT_WITH_WEIGHT aggregate expression

**Methods:**

- `fn new() -> Self` - Create a new [`ApproxPercentileContWithWeight`] aggregate function.

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>` - See [`TDigest::to_scalar_state()`] for a description of the serialized
  - `fn supports_within_group_clause(self: &Self) -> bool`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &ApproxPercentileContWithWeight) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::approx_percentile_cont_with_weight::ApproxPercentileWithWeightAccumulator

*Struct*

**Methods:**

- `fn new(approx_percentile_cont_accumulator: ApproxPercentileAccumulator) -> Self`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::approx_percentile_cont_with_weight::approx_percentile_cont_with_weight

*Function*

Computes the approximate percentile continuous with weight of a set of numbers

```rust
fn approx_percentile_cont_with_weight(order_by: datafusion_expr::expr::Sort, weight: datafusion_expr::Expr, percentile: datafusion_expr::Expr, centroids: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::approx_percentile_cont_with_weight::approx_percentile_cont_with_weight_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxPercentileContWithWeight`]

```rust
fn approx_percentile_cont_with_weight_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



