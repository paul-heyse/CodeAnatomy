**datafusion_expr_common**

# Module: datafusion_expr_common

## Contents

**Modules**

- [`accumulator`](#accumulator) - Accumulator module contains the trait definition for aggregation function's accumulators.
- [`casts`](#casts) - Utilities for casting scalar literals to different data types
- [`columnar_value`](#columnar_value) - [`ColumnarValue`] represents the result of evaluating an expression.
- [`dyn_eq`](#dyn_eq)
- [`groups_accumulator`](#groups_accumulator) - Vectorized [`GroupsAccumulator`]
- [`interval_arithmetic`](#interval_arithmetic) - Interval arithmetic library
- [`operator`](#operator)
- [`signature`](#signature) - Function signatures: [`Volatility`], [`Signature`] and [`TypeSignature`]
- [`sort_properties`](#sort_properties)
- [`statistics`](#statistics)
- [`type_coercion`](#type_coercion)

---

## Module: accumulator

Accumulator module contains the trait definition for aggregation function's accumulators.



## Module: casts

Utilities for casting scalar literals to different data types

This module contains functions for casting ScalarValue literals
to different data types, originally extracted from the optimizer's
unwrap_cast module to be shared between logical and physical layers.



## Module: columnar_value

[`ColumnarValue`] represents the result of evaluating an expression.



## Module: dyn_eq



## Module: groups_accumulator

Vectorized [`GroupsAccumulator`]



## Module: interval_arithmetic

Interval arithmetic library



## Module: operator



## Module: signature

Function signatures: [`Volatility`], [`Signature`] and [`TypeSignature`]



## Module: sort_properties



## Module: statistics



## Module: type_coercion



