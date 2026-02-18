**datafusion_functions_aggregate**

# Module: datafusion_functions_aggregate

## Contents

**Modules**

- [`approx_distinct`](#approx_distinct) - Defines physical expressions that can evaluated at runtime during query execution
- [`approx_median`](#approx_median) - Defines physical expressions for APPROX_MEDIAN that can be evaluated MEDIAN at runtime during query execution
- [`approx_percentile_cont`](#approx_percentile_cont)
- [`approx_percentile_cont_with_weight`](#approx_percentile_cont_with_weight)
- [`array_agg`](#array_agg) - `ARRAY_AGG` aggregate implementation: [`ArrayAgg`]
- [`average`](#average) - Defines `Avg` & `Mean` aggregate & accumulators
- [`bit_and_or_xor`](#bit_and_or_xor) - Defines `BitAnd`, `BitOr`, `BitXor` and `BitXor DISTINCT` aggregate accumulators
- [`bool_and_or`](#bool_and_or) - Defines physical expressions that can evaluated at runtime during query execution
- [`correlation`](#correlation) - [`Correlation`]: correlation sample aggregations.
- [`count`](#count)
- [`covariance`](#covariance) - [`CovarianceSample`]: covariance sample aggregations.
- [`expr_fn`](#expr_fn) - Fluent-style API for creating `Expr`s
- [`first_last`](#first_last) - Defines the FIRST_VALUE/LAST_VALUE aggregations.
- [`grouping`](#grouping) - Defines physical expressions that can evaluated at runtime during query execution
- [`hyperloglog`](#hyperloglog) - # HyperLogLog
- [`macros`](#macros)
- [`median`](#median)
- [`min_max`](#min_max) - [`Max`] and [`MaxAccumulator`] accumulator for the `max` function
- [`nth_value`](#nth_value) - Defines NTH_VALUE aggregate expression which may specify ordering requirement
- [`percentile_cont`](#percentile_cont)
- [`planner`](#planner) - SQL planning extensions like [`AggregateFunctionPlanner`]
- [`regr`](#regr) - Defines physical expressions that can evaluated at runtime during query execution
- [`stddev`](#stddev) - Defines physical expressions that can evaluated at runtime during query execution
- [`string_agg`](#string_agg) - [`StringAgg`] accumulator for the `string_agg` function
- [`sum`](#sum) - Defines `SUM` and `SUM DISTINCT` aggregate accumulators
- [`variance`](#variance) - [`VarianceSample`]: variance sample aggregations.

**Macros**

- [`create_func`](#create_func)
- [`make_udaf_expr`](#make_udaf_expr)
- [`make_udaf_expr_and_func`](#make_udaf_expr_and_func)

**Functions**

- [`all_default_aggregate_functions`](#all_default_aggregate_functions) - Returns all default aggregate functions
- [`register_all`](#register_all) - Registers all enabled packages with a [`FunctionRegistry`]

---

## datafusion_functions_aggregate::all_default_aggregate_functions

*Function*

Returns all default aggregate functions

```rust
fn all_default_aggregate_functions() -> Vec<std::sync::Arc<datafusion_expr::AggregateUDF>>
```



## Module: approx_distinct

Defines physical expressions that can evaluated at runtime during query execution



## Module: approx_median

Defines physical expressions for APPROX_MEDIAN that can be evaluated MEDIAN at runtime during query execution



## Module: approx_percentile_cont



## Module: approx_percentile_cont_with_weight



## Module: array_agg

`ARRAY_AGG` aggregate implementation: [`ArrayAgg`]



## Module: average

Defines `Avg` & `Mean` aggregate & accumulators



## Module: bit_and_or_xor

Defines `BitAnd`, `BitOr`, `BitXor` and `BitXor DISTINCT` aggregate accumulators



## Module: bool_and_or

Defines physical expressions that can evaluated at runtime during query execution



## Module: correlation

[`Correlation`]: correlation sample aggregations.



## Module: count



## Module: covariance

[`CovarianceSample`]: covariance sample aggregations.



## datafusion_functions_aggregate::create_func

*Declarative Macro*

```rust
macro_rules! create_func {
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident) => { ... };
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident, $CREATE:expr) => { ... };
}
```



## Module: expr_fn

Fluent-style API for creating `Expr`s



## Module: first_last

Defines the FIRST_VALUE/LAST_VALUE aggregations.



## Module: grouping

Defines physical expressions that can evaluated at runtime during query execution



## Module: hyperloglog

# HyperLogLog

`hyperloglog` is a module that contains a modified version
of [redis's implementation](https://github.com/redis/redis/blob/4930d19e70c391750479951022e207e19111eb55/src/hyperloglog.c)
with some modification based on strong assumption of usage
within datafusion, so that function can
be efficiently implemented.

Specifically, like Redis's version, this HLL structure uses
2**14 = 16384 registers, which means the standard error is
1.04/(16384**0.5) = 0.8125%. Unlike Redis, the register takes
up full [`u8`] size instead of a raw int* and thus saves some
tricky bit shifting techniques used in the original version.
This results in a memory usage increase from 12Kib to 16Kib.
Also only the dense version is adopted, so there's no automatic
conversion, largely to simplify the code.

This module also borrows some code structure from [pdatastructs.rs](https://github.com/crepererum/pdatastructs.rs/blob/3997ed50f6b6871c9e53c4c5e0f48f431405fc63/src/hyperloglog.rs).



## Module: macros



## datafusion_functions_aggregate::make_udaf_expr

*Declarative Macro*

```rust
macro_rules! make_udaf_expr {
    ($EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => { ... };
}
```



## datafusion_functions_aggregate::make_udaf_expr_and_func

*Declarative Macro*

```rust
macro_rules! make_udaf_expr_and_func {
    ($UDAF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => { ... };
    ($UDAF:ty, $EXPR_FN:ident, $DOC:expr, $AGGREGATE_UDF_FN:ident) => { ... };
}
```



## Module: median



## Module: min_max

[`Max`] and [`MaxAccumulator`] accumulator for the `max` function
[`Min`] and [`MinAccumulator`] accumulator for the `min` function



## Module: nth_value

Defines NTH_VALUE aggregate expression which may specify ordering requirement
that can evaluated at runtime during query execution



## Module: percentile_cont



## Module: planner

SQL planning extensions like [`AggregateFunctionPlanner`]



## datafusion_functions_aggregate::register_all

*Function*

Registers all enabled packages with a [`FunctionRegistry`]

```rust
fn register_all(registry: & mut dyn FunctionRegistry) -> datafusion_common::Result<()>
```



## Module: regr

Defines physical expressions that can evaluated at runtime during query execution



## Module: stddev

Defines physical expressions that can evaluated at runtime during query execution



## Module: string_agg

[`StringAgg`] accumulator for the `string_agg` function



## Module: sum

Defines `SUM` and `SUM DISTINCT` aggregate accumulators



## Module: variance

[`VarianceSample`]: variance sample aggregations.
[`VariancePopulation`]: variance population aggregations.



