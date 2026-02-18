**datafusion_functions_aggregate > correlation**

# Module: correlation

## Contents

**Structs**

- [`Correlation`](#correlation)
- [`CorrelationAccumulator`](#correlationaccumulator) - An accumulator to compute correlation
- [`CorrelationGroupsAccumulator`](#correlationgroupsaccumulator)

**Functions**

- [`corr`](#corr) - Correlation between two numeric values.
- [`corr_udaf`](#corr_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Correlation`]

---

## datafusion_functions_aggregate::correlation::Correlation

*Struct*

**Methods:**

- `fn new() -> Self` - Create a new CORR aggregate function

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Correlation) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any` - Return a reference to Any that can be used for downcasting
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
  - `fn groups_accumulator_supported(self: &Self, _args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::correlation::CorrelationAccumulator

*Struct*

An accumulator to compute correlation

**Methods:**

- `fn try_new() -> Result<Self>` - Creates a new `CorrelationAccumulator`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Accumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`



## datafusion_functions_aggregate::correlation::CorrelationGroupsAccumulator

*Struct*

**Methods:**

- `fn new() -> Self`

**Trait Implementations:**

- **GroupsAccumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> Result<()>`
  - `fn evaluate(self: & mut Self, emit_to: EmitTo) -> Result<ArrayRef>`
  - `fn state(self: & mut Self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>`
  - `fn merge_batch(self: & mut Self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> Result<()>`
  - `fn size(self: &Self) -> usize`
- **Default**
  - `fn default() -> CorrelationGroupsAccumulator`



## datafusion_functions_aggregate::correlation::corr

*Function*

Correlation between two numeric values.

```rust
fn corr(y: datafusion_expr::Expr, x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::correlation::corr_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Correlation`]

```rust
fn corr_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



