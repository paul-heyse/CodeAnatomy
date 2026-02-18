**datafusion_functions_window > ntile**

# Module: ntile

## Contents

**Structs**

- [`Ntile`](#ntile)

**Functions**

- [`ntile`](#ntile) - Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
- [`ntile_udwf`](#ntile_udwf) - Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`ntile`].

---

## datafusion_functions_window::ntile::Ntile

*Struct*

**Methods:**

- `fn new() -> Self` - Create a new `ntile` function

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Ntile) -> bool`
- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn partition_evaluator(self: &Self, partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **Default**
  - `fn default() -> Self`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_window::ntile::ntile

*Function*

Create a [`WindowFunction`](datafusion_expr::Expr::WindowFunction) expression for
`Ntile` user-defined window function.

Integer ranging from 1 to the argument value, dividing the partition as equally as possible.

```rust
fn ntile(arg: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_window::ntile::ntile_udwf

*Function*

Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [`ntile`].

Integer ranging from 1 to the argument value, dividing the partition as equally as possible.

```rust
fn ntile_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF>
```



