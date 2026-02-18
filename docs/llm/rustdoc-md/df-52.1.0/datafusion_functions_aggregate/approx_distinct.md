**datafusion_functions_aggregate > approx_distinct**

# Module: approx_distinct

## Contents

**Structs**

- [`ApproxDistinct`](#approxdistinct)

**Functions**

- [`approx_distinct`](#approx_distinct) - approximate number of distinct input values
- [`approx_distinct_udaf`](#approx_distinct_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxDistinct`]

---

## datafusion_functions_aggregate::approx_distinct::ApproxDistinct

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ApproxDistinct) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_aggregate::approx_distinct::approx_distinct

*Function*

approximate number of distinct input values

```rust
fn approx_distinct(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`ApproxDistinct`]

```rust
fn approx_distinct_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



