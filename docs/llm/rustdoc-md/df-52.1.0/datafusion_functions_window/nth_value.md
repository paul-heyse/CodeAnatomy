**datafusion_functions_window > nth_value**

# Module: nth_value

## Contents

**Structs**

- [`NthValue`](#nthvalue)
- [`NthValueState`](#nthvaluestate)

**Enums**

- [`NthValueKind`](#nthvaluekind) - Tag to differentiate special use cases of the NTH_VALUE built-in window function.

**Functions**

- [`first_value`](#first_value) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`first_value_udwf`](#first_value_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`first_value`].
- [`last_value`](#last_value) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`last_value_udwf`](#last_value_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`last_value`].
- [`nth_value`](#nth_value) - Create an expression to represent the `nth_value` window function
- [`nth_value_udwf`](#nth_value_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`nth_value`].

---

## datafusion_functions_window::nth_value::NthValue

*Struct*

**Methods:**

- `fn new(kind: NthValueKind) -> Self` - Create a new `nth_value` function
- `fn first() -> Self`
- `fn last() -> Self`
- `fn nth() -> Self`
- `fn kind(self: &Self) -> &NthValueKind`

**Traits:** Eq

**Trait Implementations:**

- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn partition_evaluator(self: &Self, partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn reverse_expr(self: &Self) -> ReversedUDWF`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **PartialEq**
  - `fn eq(self: &Self, other: &NthValue) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_window::nth_value::NthValueKind

*Enum*

Tag to differentiate special use cases of the NTH_VALUE built-in window function.

**Variants:**
- `First`
- `Last`
- `Nth`

**Traits:** Eq, Copy

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> NthValueKind`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &NthValueKind) -> bool`



## datafusion_functions_window::nth_value::NthValueState

*Struct*

**Fields:**
- `finalized_result: Option<datafusion_common::ScalarValue>`
- `kind: NthValueKind`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> NthValueState`



## datafusion_functions_window::nth_value::first_value

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`First` user-defined window function.

Returns the first value in the window frame

```rust
fn first_value(arg: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_window::nth_value::first_value_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`first_value`].

Returns the first value in the window frame

```rust
fn first_value_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



## datafusion_functions_window::nth_value::last_value

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`Last` user-defined window function.

Returns the last value in the window frame

```rust
fn last_value(arg: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_window::nth_value::last_value_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`last_value`].

Returns the last value in the window frame

```rust
fn last_value_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



## datafusion_functions_window::nth_value::nth_value

*Function*

Create an expression to represent the `nth_value` window function

```rust
fn nth_value(arg: datafusion_expr::Expr, n: i64) -> datafusion_expr::Expr
```



## datafusion_functions_window::nth_value::nth_value_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`nth_value`].

Returns the nth value in the window frame

```rust
fn nth_value_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



