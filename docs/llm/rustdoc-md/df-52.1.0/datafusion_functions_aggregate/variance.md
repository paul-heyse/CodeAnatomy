**datafusion_functions_aggregate > variance**

# Module: variance

## Contents

**Structs**

- [`VarianceAccumulator`](#varianceaccumulator) - An accumulator to compute variance
- [`VarianceGroupsAccumulator`](#variancegroupsaccumulator)
- [`VariancePopulation`](#variancepopulation)
- [`VarianceSample`](#variancesample)

**Functions**

- [`var_pop`](#var_pop) - Computes the population variance.
- [`var_pop_udaf`](#var_pop_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`VariancePopulation`]
- [`var_samp_udaf`](#var_samp_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`VarianceSample`]
- [`var_sample`](#var_sample) - Computes the sample variance.

---

## datafusion_functions_aggregate::variance::VarianceAccumulator

*Struct*

An accumulator to compute variance
The algorithm used is an online implementation and numerically stable. It is based on this paper:
Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.

The algorithm has been analyzed here:
Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

**Methods:**

- `fn try_new(s_type: StatsType) -> Result<Self>` - Creates a new `VarianceAccumulator`
- `fn get_count(self: &Self) -> u64`
- `fn get_mean(self: &Self) -> f64`
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



## datafusion_functions_aggregate::variance::VarianceGroupsAccumulator

*Struct*

**Methods:**

- `fn new(s_type: StatsType) -> Self`
- `fn variance(self: & mut Self, emit_to: datafusion_expr::EmitTo) -> (Vec<f64>, NullBuffer)`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **GroupsAccumulator**
  - `fn update_batch(self: & mut Self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> Result<()>`
  - `fn merge_batch(self: & mut Self, values: &[ArrayRef], group_indices: &[usize], _opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> Result<()>`
  - `fn evaluate(self: & mut Self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef>`
  - `fn state(self: & mut Self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>>`
  - `fn size(self: &Self) -> usize`



## datafusion_functions_aggregate::variance::VariancePopulation

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn groups_accumulator_supported(self: &Self, acc_args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &VariancePopulation) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::variance::VarianceSample

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &VarianceSample) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn groups_accumulator_supported(self: &Self, acc_args: AccumulatorArgs) -> bool`
  - `fn create_groups_accumulator(self: &Self, _args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`



## datafusion_functions_aggregate::variance::var_pop

*Function*

Computes the population variance.

```rust
fn var_pop(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::variance::var_pop_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`VariancePopulation`]

```rust
fn var_pop_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::variance::var_samp_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`VarianceSample`]

```rust
fn var_samp_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::variance::var_sample

*Function*

Computes the sample variance.

```rust
fn var_sample(expression: datafusion_expr::Expr) -> datafusion_expr::Expr
```



