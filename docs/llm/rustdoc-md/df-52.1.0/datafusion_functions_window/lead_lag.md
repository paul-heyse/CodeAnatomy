**datafusion_functions_window > lead_lag**

# Module: lead_lag

## Contents

**Structs**

- [`WindowShift`](#windowshift) - window shift expression

**Enums**

- [`WindowShiftKind`](#windowshiftkind)

**Functions**

- [`lag`](#lag) - Create an expression to represent the `lag` window function
- [`lag_udwf`](#lag_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`lag`].
- [`lead`](#lead) - Create an expression to represent the `lead` window function
- [`lead_udwf`](#lead_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`lead`].

---

## datafusion_functions_window::lead_lag::WindowShift

*Struct*

window shift expression

**Methods:**

- `fn lag() -> Self`
- `fn lead() -> Self`
- `fn kind(self: &Self) -> &WindowShiftKind`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn expressions(self: &Self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>>` - Handles the case where `NULL` expression is passed as an
  - `fn partition_evaluator(self: &Self, partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn reverse_expr(self: &Self) -> ReversedUDWF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn limit_effect(self: &Self, args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **PartialEq**
  - `fn eq(self: &Self, other: &WindowShift) -> bool`



## datafusion_functions_window::lead_lag::WindowShiftKind

*Enum*

**Variants:**
- `Lag`
- `Lead`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &WindowShiftKind) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_window::lead_lag::lag

*Function*

Create an expression to represent the `lag` window function

returns value evaluated at the row that is offset rows before the current row within the partition;
if there is no such row, instead return default (which must be of the same type as value).
Both offset and default are evaluated with respect to the current row.
If omitted, offset defaults to 1 and default to null

```rust
fn lag(arg: datafusion_expr::Expr, shift_offset: Option<i64>, default_value: Option<datafusion_common::ScalarValue>) -> datafusion_expr::Expr
```



## datafusion_functions_window::lead_lag::lag_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`lag`].

Returns the row value that precedes the current row by a specified offset within partition. If no such row exists, then returns the default value.

```rust
fn lag_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



## datafusion_functions_window::lead_lag::lead

*Function*

Create an expression to represent the `lead` window function

returns value evaluated at the row that is offset rows after the current row within the partition;
if there is no such row, instead return default (which must be of the same type as value).
Both offset and default are evaluated with respect to the current row.
If omitted, offset defaults to 1 and default to null

```rust
fn lead(arg: datafusion_expr::Expr, shift_offset: Option<i64>, default_value: Option<datafusion_common::ScalarValue>) -> datafusion_expr::Expr
```



## datafusion_functions_window::lead_lag::lead_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`lead`].

Returns the value from a row that follows the current row by a specified offset within the partition. If no such row exists, then returns the default value.

```rust
fn lead_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



