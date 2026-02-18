**datafusion_functions_aggregate > string_agg**

# Module: string_agg

## Contents

**Structs**

- [`StringAgg`](#stringagg) - STRING_AGG aggregate expression

**Functions**

- [`string_agg`](#string_agg) - Concatenates the values of string expressions and places separator values between them
- [`string_agg_udaf`](#string_agg_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`StringAgg`]

---

## datafusion_functions_aggregate::string_agg::StringAgg

*Struct*

STRING_AGG aggregate expression

**Methods:**

- `fn new() -> Self` - Create a new StringAgg aggregate function

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn reverse_expr(self: &Self) -> datafusion_expr::ReversedUDAF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &StringAgg) -> bool`



## datafusion_functions_aggregate::string_agg::string_agg

*Function*

Concatenates the values of string expressions and places separator values between them

```rust
fn string_agg(expr: datafusion_expr::Expr, delimiter: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::string_agg::string_agg_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`StringAgg`]

```rust
fn string_agg_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



