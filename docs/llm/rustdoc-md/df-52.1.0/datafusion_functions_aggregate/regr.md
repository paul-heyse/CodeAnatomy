**datafusion_functions_aggregate > regr**

# Module: regr

## Contents

**Structs**

- [`Regr`](#regr)
- [`RegrAccumulator`](#regraccumulator) - `RegrAccumulator` is used to compute linear regression aggregate functions

**Enums**

- [`RegrType`](#regrtype)

**Functions**

- [`regr_avgx`](#regr_avgx) - Compute a linear regression of type [RegrType::AvgX]
- [`regr_avgx_udaf`](#regr_avgx_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_avgx`]
- [`regr_avgy`](#regr_avgy) - Compute a linear regression of type [RegrType::AvgY]
- [`regr_avgy_udaf`](#regr_avgy_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_avgy`]
- [`regr_count`](#regr_count) - Compute a linear regression of type [RegrType::Count]
- [`regr_count_udaf`](#regr_count_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_count`]
- [`regr_intercept`](#regr_intercept) - Compute a linear regression of type [RegrType::Intercept]
- [`regr_intercept_udaf`](#regr_intercept_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_intercept`]
- [`regr_r2`](#regr_r2) - Compute a linear regression of type [RegrType::R2]
- [`regr_r2_udaf`](#regr_r2_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_r2`]
- [`regr_slope`](#regr_slope) - Compute a linear regression of type [RegrType::Slope]
- [`regr_slope_udaf`](#regr_slope_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_slope`]
- [`regr_sxx`](#regr_sxx) - Compute a linear regression of type [RegrType::SXX]
- [`regr_sxx_udaf`](#regr_sxx_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_sxx`]
- [`regr_sxy`](#regr_sxy) - Compute a linear regression of type [RegrType::SXY]
- [`regr_sxy_udaf`](#regr_sxy_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_sxy`]
- [`regr_syy`](#regr_syy) - Compute a linear regression of type [RegrType::SYY]
- [`regr_syy_udaf`](#regr_syy_udaf) - AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_syy`]

---

## datafusion_functions_aggregate::regr::Regr

*Struct*

**Methods:**

- `fn new(regr_type: RegrType, func_name: &'static str) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>`
  - `fn state_fields(self: &Self, args: StateFieldsArgs) -> Result<Vec<FieldRef>>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Regr) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::regr::RegrAccumulator

*Struct*

`RegrAccumulator` is used to compute linear regression aggregate functions
by maintaining statistics needed to compute them in an online fashion.

This struct uses Welford's online algorithm for calculating variance and covariance:
<https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>

Given the statistics, the following aggregate functions can be calculated:

- `regr_slope(y, x)`: Slope of the linear regression line, calculated as:
  cov_pop(x, y) / var_pop(x).
  It represents the expected change in Y for a one-unit change in X.

- `regr_intercept(y, x)`: Intercept of the linear regression line, calculated as:
  mean_y - (regr_slope(y, x) * mean_x).
  It represents the expected value of Y when X is 0.

- `regr_count(y, x)`: Count of the non-null(both x and y) input rows.

- `regr_r2(y, x)`: R-squared value (coefficient of determination), calculated as:
  (cov_pop(x, y) ^ 2) / (var_pop(x) * var_pop(y)).
  It provides a measure of how well the model's predictions match the observed data.

- `regr_avgx(y, x)`: Average of the independent variable X, calculated as: mean_x.

- `regr_avgy(y, x)`: Average of the dependent variable Y, calculated as: mean_y.

- `regr_sxx(y, x)`: Sum of squares of the independent variable X, calculated as:
  m2_x.

- `regr_syy(y, x)`: Sum of squares of the dependent variable Y, calculated as:
  m2_y.

- `regr_sxy(y, x)`: Sum of products of paired values, calculated as:
  algo_const.

Here's how the statistics maintained in this struct are calculated:
- `cov_pop(x, y)`: algo_const / count.
- `var_pop(x)`: m2_x / count.
- `var_pop(y)`: m2_y / count.

**Methods:**

- `fn try_new(regr_type: &RegrType) -> Result<Self>` - Creates a new `RegrAccumulator`

**Trait Implementations:**

- **Accumulator**
  - `fn state(self: & mut Self) -> Result<Vec<ScalarValue>>`
  - `fn update_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn supports_retract_batch(self: &Self) -> bool`
  - `fn retract_batch(self: & mut Self, values: &[ArrayRef]) -> Result<()>`
  - `fn merge_batch(self: & mut Self, states: &[ArrayRef]) -> Result<()>`
  - `fn evaluate(self: & mut Self) -> Result<ScalarValue>`
  - `fn size(self: &Self) -> usize`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions_aggregate::regr::RegrType

*Enum*

**Variants:**
- `Slope` - Variant for `regr_slope` aggregate expression
- `Intercept` - Variant for `regr_intercept` aggregate expression
- `Count` - Variant for `regr_count` aggregate expression
- `R2` - Variant for `regr_r2` aggregate expression
- `AvgX` - Variant for `regr_avgx` aggregate expression
- `AvgY` - Variant for `regr_avgy` aggregate expression
- `SXX` - Variant for `regr_sxx` aggregate expression
- `SYY` - Variant for `regr_syy` aggregate expression
- `SXY` - Variant for `regr_sxy` aggregate expression

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RegrType`
- **PartialEq**
  - `fn eq(self: &Self, other: &RegrType) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions_aggregate::regr::regr_avgx

*Function*

Compute a linear regression of type [RegrType::AvgX]

```rust
fn regr_avgx(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_avgx_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_avgx`]

```rust
fn regr_avgx_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_avgy

*Function*

Compute a linear regression of type [RegrType::AvgY]

```rust
fn regr_avgy(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_avgy_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_avgy`]

```rust
fn regr_avgy_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_count

*Function*

Compute a linear regression of type [RegrType::Count]

```rust
fn regr_count(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_count_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_count`]

```rust
fn regr_count_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_intercept

*Function*

Compute a linear regression of type [RegrType::Intercept]

```rust
fn regr_intercept(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_intercept_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_intercept`]

```rust
fn regr_intercept_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_r2

*Function*

Compute a linear regression of type [RegrType::R2]

```rust
fn regr_r2(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_r2_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_r2`]

```rust
fn regr_r2_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_slope

*Function*

Compute a linear regression of type [RegrType::Slope]

```rust
fn regr_slope(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_slope_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_slope`]

```rust
fn regr_slope_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_sxx

*Function*

Compute a linear regression of type [RegrType::SXX]

```rust
fn regr_sxx(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_sxx_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_sxx`]

```rust
fn regr_sxx_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_sxy

*Function*

Compute a linear regression of type [RegrType::SXY]

```rust
fn regr_sxy(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_sxy_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_sxy`]

```rust
fn regr_sxy_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



## datafusion_functions_aggregate::regr::regr_syy

*Function*

Compute a linear regression of type [RegrType::SYY]

```rust
fn regr_syy(expr_y: datafusion_expr::Expr, expr_x: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions_aggregate::regr::regr_syy_udaf

*Function*

AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`regr_syy`]

```rust
fn regr_syy_udaf() -> std::sync::Arc<datafusion_expr::AggregateUDF>
```



