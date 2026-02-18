**datafusion_functions_aggregate > grouping**

# Module: grouping

## Contents

**Structs**

- [`Grouping`](#grouping)

**Functions**

- [`grouping`](#grouping) - Returns 1 if the data is aggregated across the specified column or 0 for not aggregated in the result set.
- [`grouping_udaf`](#grouping_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Grouping`]

---

## datafusion_functions_aggregate::grouping::Grouping

*Struct*

**Methods:**

- `fn new() -> Self` - Create a new GROUPING aggregate function.

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Grouping) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



## datafusion_functions_aggregate::grouping::grouping

*Function*

Returns 1 if the data is aggregated across the specified column or 0 for not aggregated in the result set.

```rust
fn grouping(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::grouping::grouping_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Grouping`]

```rust
fn grouping_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



