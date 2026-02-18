**datafusion_functions_aggregate > covariance**

# Module: covariance

## Contents

**Structs**

- [`CovarianceAccumulator`](#covarianceaccumulator) - An accumulator to compute covariance
- [`CovariancePopulation`](#covariancepopulation)
- [`CovarianceSample`](#covariancesample)

**Functions**

- [`covar_pop`](#covar_pop) - Computes the population covariance.
- [`covar_pop_udaf`](#covar_pop_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`CovariancePopulation`]
- [`covar_samp`](#covar_samp) - Computes the sample covariance.
- [`covar_samp_udaf`](#covar_samp_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`CovarianceSample`]

---

## datafusion_functions_aggregate::covariance::CovarianceAccumulator

*Struct*

An accumulator to compute covariance
The algorithm used is an online implementation and numerically stable. It is derived from the following paper
for calculating variance:
Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.

The algorithm has been analyzed here:
Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

Though it is not covered in the original paper but is based on the same idea, as a result the algorithm is online,
parallelize and numerically stable.

**Methods:**

- `fn try_new(s_type: StatsType) -> Result<Self>` - Creates a new `CovarianceAccumulator`
- `fn get_count(self: &Self) -> u64`
- `fn get_mean1(self: &Self) -> f64`
- `fn get_mean2(self: &Self) -> f64`
- `fn get_algo_const(self: &Self) -> f64`

**Trait Implementations:**

- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::covariance::CovariancePopulation

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &CovariancePopulation) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::covariance::CovarianceSample

*Struct*

**Methods:**

- `fn new() -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn std::any::Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn aliases(self: &Self) -> &[String]`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **PartialEq**
  - `fn eq(self: &Self, other: &CovarianceSample) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::covariance::covar_pop

*Function*

Computes the population covariance.

```rust
fn covar_pop(y: datafusion_expr::Expr, x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::covariance::covar_pop_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`CovariancePopulation`]

```rust
fn covar_pop_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::covariance::covar_samp

*Function*

Computes the sample covariance.

```rust
fn covar_samp(y: datafusion_expr::Expr, x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::covariance::covar_samp_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`CovarianceSample`]

```rust
fn covar_samp_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



