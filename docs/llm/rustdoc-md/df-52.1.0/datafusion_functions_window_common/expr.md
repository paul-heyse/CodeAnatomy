**datafusion_functions_window_common > expr**

# Module: expr

## Contents

**Structs**

- [`ExpressionArgs`](#expressionargs) - Arguments passed to user-defined window function

---

## datafusion_functions_window_common::expr::ExpressionArgs

*Struct*

Arguments passed to user-defined window function

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(input_exprs: &'a [Arc<dyn PhysicalExpr>], input_fields: &'a [FieldRef]) -> Self` - Create an instance of [`ExpressionArgs`].
- `fn input_exprs(self: &Self) -> &'a [Arc<dyn PhysicalExpr>]` - Returns the expressions passed as arguments to the user-defined
- `fn input_fields(self: &Self) -> &'a [FieldRef]` - Returns the [`FieldRef`]s corresponding to the input expressions

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> ExpressionArgs<'a>`



