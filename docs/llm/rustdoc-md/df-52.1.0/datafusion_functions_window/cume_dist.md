**datafusion_functions_window > cume_dist**

# Module: cume_dist

## Contents

**Structs**

- [`CumeDist`](#cumedist) - CumeDist calculates the cume_dist in the window function with order by

**Functions**

- [`cume_dist`](#cume_dist) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`cume_dist_udwf`](#cume_dist_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`cume_dist`].

---

## datafusion_functions_window::cume_dist::CumeDist

*Struct*

CumeDist calculates the cume_dist in the window function with order by

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &CumeDist) -> bool`
- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any` - Return a reference to Any that can be used for downcasting
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn partition_evaluator(self: &Self, _partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`



## datafusion_functions_window::cume_dist::cume_dist

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`CumeDist` user-defined window function.

Calculates the cumulative distribution of a value in a group of values.

```rust
fn cume_dist() -> datafusion_expr::Expr
```



## datafusion_functions_window::cume_dist::cume_dist_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`cume_dist`].

Calculates the cumulative distribution of a value in a group of values.

```rust
fn cume_dist_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



