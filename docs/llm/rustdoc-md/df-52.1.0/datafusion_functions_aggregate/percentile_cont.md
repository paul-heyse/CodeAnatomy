**datafusion_functions_aggregate > percentile_cont**

# Module: percentile_cont

## Contents

**Structs**

- [`PercentileCont`](#percentilecont) - PERCENTILE_CONT aggregate expression. This uses an exact calculation and stores all values

**Functions**

- [`percentile_cont`](#percentile_cont) - Computes the exact percentile continuous of a set of numbers
- [`percentile_cont_udaf`](#percentile_cont_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`PercentileCont`]

---

## datafusion_functions_aggregate::percentile_cont::PercentileCont

*Struct*

PERCENTILE_CONT aggregate expression. This uses an exact calculation and stores all values
in memory before computing the result. If an approximation is sufficient then
APPROX_PERCENTILE_CONT provides a much more efficient solution.

If using the distinct variation, the memory usage will be similarly high if the
cardinality is high as it stores all distinct values in memory before computing the
result, but if cardinality is low then memory usage will also be lower.

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn simplify(self: &Self) -> Option<AggregateFunctionSimplification>`
  - `fn supports_within_group_clause(self: &Self) -> bool`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &PercentileCont) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_aggregate::percentile_cont::percentile_cont

*Function*

Computes the exact percentile continuous of a set of numbers

```rust
fn percentile_cont(order_by: datafusion_expr::expr::Sort, percentile: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::percentile_cont::percentile_cont_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`PercentileCont`]

```rust
fn percentile_cont_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



