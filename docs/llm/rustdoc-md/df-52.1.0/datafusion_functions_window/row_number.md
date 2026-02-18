**datafusion_functions_window > row_number**

# Module: row_number

## Contents

**Structs**

- [`RowNumber`](#rownumber) - row_number expression

**Functions**

- [`row_number`](#row_number) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`row_number_udwf`](#row_number_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`row_number`].

---

## datafusion_functions_window::row_number::RowNumber

*Struct*

row_number expression

**Methods:**

- `fn new() -> Self` - Create a new `row_number` function

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &RowNumber) -> bool`
- **Default**
  - `fn default() -> Self`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn partition_evaluator(self: &Self, _partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn sort_options(self: &Self) -> Option<SortOptions>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`



## datafusion_functions_window::row_number::row_number

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`RowNumber` user-defined window function.

Returns a unique row number for each row in window partition beginning at 1.

```rust
fn row_number() -> datafusion_expr::Expr
```



## datafusion_functions_window::row_number::row_number_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`row_number`].

Returns a unique row number for each row in window partition beginning at 1.

```rust
fn row_number_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



