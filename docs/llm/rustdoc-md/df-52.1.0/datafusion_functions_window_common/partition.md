**datafusion_functions_window_common > partition**

# Module: partition

## Contents

**Structs**

- [`PartitionEvaluatorArgs`](#partitionevaluatorargs) - Arguments passed to created user-defined window function state

---

## datafusion_functions_window_common::partition::PartitionEvaluatorArgs

*Struct*

Arguments passed to created user-defined window function state
during physical execution.

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(input_exprs: &'a [Arc<dyn PhysicalExpr>], input_fields: &'a [FieldRef], is_reversed: bool, ignore_nulls: bool) -> Self` - Create an instance of [`PartitionEvaluatorArgs`].
- `fn input_exprs(self: &Self) -> &'a [Arc<dyn PhysicalExpr>]` - Returns the expressions passed as arguments to the user-defined
- `fn input_fields(self: &Self) -> &'a [FieldRef]` - Returns the [`FieldRef`]s corresponding to the input expressions
- `fn is_reversed(self: &Self) -> bool` - Returns `true` when the user-defined window function is
- `fn ignore_nulls(self: &Self) -> bool` - Returns `true` when `IGNORE NULLS` is specified, otherwise

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> PartitionEvaluatorArgs<'a>`



