**datafusion_functions_aggregate > stddev**

# Module: stddev

## Contents

**Structs**

- [`Stddev`](#stddev) - STDDEV and STDDEV_SAMP (standard deviation) aggregate expression
- [`StddevAccumulator`](#stddevaccumulator) - An accumulator to compute the average
- [`StddevGroupsAccumulator`](#stddevgroupsaccumulator)
- [`StddevPop`](#stddevpop) - STDDEV_POP population aggregate expression

**Functions**

- [`stddev`](#stddev) - Compute the standard deviation of a set of numbers
- [`stddev_pop`](#stddev_pop) - Compute the population standard deviation of a set of numbers
- [`stddev_pop_udaf`](#stddev_pop_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`StddevPop`]
- [`stddev_udaf`](#stddev_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Stddev`]

---

## datafusion_functions_aggregate::stddev::Stddev

*Struct*

STDDEV and STDDEV_SAMP (standard deviation) aggregate expression

**Methods:**

- `fn new() -> Self` - Create a new STDDEV aggregate function

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any` - Return a reference to Any that can be used for downcasting
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn groups_accumulator_supported(self: &Self, acc_args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Stddev) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::stddev::StddevAccumulator

*Struct*

An accumulator to compute the average

**Methods:**

- `fn try_new(s_type: StatsType) -> Result<Self>` - Creates a new `StddevAccumulator`
- `fn get_m2(self: &Self) -> f64`

**Trait Implementations:**

- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
  - `fn supports_retract_batch(self: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::stddev::StddevGroupsAccumulator

*Struct*

**Methods:**

- `fn new(s_type: StatsType) -> Self`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **GroupsAccumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&arrow::array::BooleanArray>, total_num_groups: usize) -> Result<()>`
  - `fn merge_batch(self: & mut Self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&arrow::array::BooleanArray>, total_num_groups: usize) -> Result<()>`
  - `fn evaluate(self: & mut Self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef>`
  - `fn state(self: & mut Self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::stddev::StddevPop

*Struct*

STDDEV_POP population aggregate expression

**Methods:**

- `fn new() -> Self` - Create a new STDDEV_POP aggregate function

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &StddevPop) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any` - Return a reference to Any that can be used for downcasting
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn groups_accumulator_supported(self: &Self, acc_args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`



## datafusion_functions_aggregate::stddev::stddev

*Function*

Compute the standard deviation of a set of numbers

```rust
fn stddev(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::stddev::stddev_pop

*Function*

Compute the population standard deviation of a set of numbers

```rust
fn stddev_pop(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::stddev::stddev_pop_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`StddevPop`]

```rust
fn stddev_pop_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::stddev::stddev_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`Stddev`]

```rust
fn stddev_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



