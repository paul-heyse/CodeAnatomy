**datafusion_functions_aggregate > approx_median**

# Module: approx_median

## Contents

**Structs**

- [`ApproxMedian`](#approxmedian) - APPROX_MEDIAN aggregate expression

**Functions**

- [`approx_median`](#approx_median) - Computes the approximate median of a set of numbers
- [`approx_median_udaf`](#approx_median_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxMedian`]

---

## datafusion_functions_aggregate::approx_median::ApproxMedian

*Struct*

APPROX_MEDIAN aggregate expression

**Methods:**

- `fn new() -> Self` - Create a new APPROX_MEDIAN aggregate function

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ApproxMedian) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::approx_median::approx_median

*Function*

Computes the approximate median of a set of numbers

```rust
fn approx_median(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::approx_median::approx_median_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxMedian`]

```rust
fn approx_median_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



