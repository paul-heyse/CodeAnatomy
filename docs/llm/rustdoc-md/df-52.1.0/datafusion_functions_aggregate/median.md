**datafusion_functions_aggregate > median**

# Module: median

## Contents

**Structs**

- [`Median`](#median) - MEDIAN aggregate expression. If using the non-distinct variation, then this uses a

**Functions**

- [`median`](#median) - Computes the median of a set of numbers
- [`median_udaf`](#median_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Median`]

---

## datafusion_functions_aggregate::median::Median

*Struct*

MEDIAN aggregate expression. If using the non-distinct variation, then this uses a
lot of memory because all values need to be stored in memory before a result can be
computed. If an approximation is sufficient then APPROX_MEDIAN provides a much more
efficient solution.

If using the distinct variation, the memory usage will be similarly high if the
cardinality is high as it stores all distinct values in memory before computing the
result, but if cardinality is low then memory usage will also be lower.

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Median) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn groups_accumulator_supported(self: &Self, args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_aggregate::median::median

*Function*

Computes the median of a set of numbers

```rust
fn median(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::median::median_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Median`]

```rust
fn median_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



