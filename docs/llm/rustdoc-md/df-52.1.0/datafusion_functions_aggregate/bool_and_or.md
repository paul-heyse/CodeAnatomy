**datafusion_functions_aggregate > bool_and_or**

# Module: bool_and_or

## Contents

**Structs**

- [`BoolAnd`](#booland) - BOOL_AND aggregate expression
- [`BoolOr`](#boolor) - BOOL_OR aggregate expression

**Functions**

- [`bool_and`](#bool_and) - The values to combine with `AND`
- [`bool_and_udaf`](#bool_and_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`BoolAnd`]
- [`bool_or`](#bool_or) - The values to combine with `OR`
- [`bool_or_udaf`](#bool_or_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`BoolOr`]

---

## datafusion_functions_aggregate::bool_and_or::BoolAnd

*Struct*

BOOL_AND aggregate expression

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &BoolAnd) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, _args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_aggregate::bool_and_or::BoolOr

*Struct*

BOOL_OR aggregate expression

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &BoolOr) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn groups_accumulator_supported(self: &Self, _args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn order_sensitivity(self: &Self) -> AggregateOrderSensitivity`
  - `fn reverse_expr(self: &Self) -> ReversedUDAF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> BoolOr`



## datafusion_functions_aggregate::bool_and_or::bool_and

*Function*

The values to combine with `AND`

```rust
fn bool_and(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::bool_and_or::bool_and_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`BoolAnd`]

```rust
fn bool_and_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::bool_and_or::bool_or

*Function*

The values to combine with `OR`

```rust
fn bool_or(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::bool_and_or::bool_or_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`BoolOr`]

```rust
fn bool_or_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



