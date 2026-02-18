**datafusion_expr**

# Module: datafusion_expr

## Contents

**Modules**

- [`arguments`](#arguments) - Argument resolution logic for named function parameters
- [`async_udf`](#async_udf)
- [`conditional_expressions`](#conditional_expressions) - Conditional expressions
- [`dml`](#dml) - DML (Data Manipulation Language) types for DELETE, UPDATE operations.
- [`execution_props`](#execution_props)
- [`expr`](#expr) - Logical Expressions: [`Expr`]
- [`expr_fn`](#expr_fn) - Functions for creating logical expressions
- [`expr_rewriter`](#expr_rewriter) - Expression rewriter
- [`expr_schema`](#expr_schema)
- [`function`](#function) - Function module contains typing and signature for built-in and user defined functions.
- [`groups_accumulator`](#groups_accumulator)
- [`interval_arithmetic`](#interval_arithmetic)
- [`logical_plan`](#logical_plan)
- [`planner`](#planner) - [`ContextProvider`] and [`ExprPlanner`] APIs to customize SQL query planning
- [`ptr_eq`](#ptr_eq)
- [`registry`](#registry) - FunctionRegistry trait
- [`select_expr`](#select_expr)
- [`simplify`](#simplify) - Structs and traits to provide the information needed for expression simplification.
- [`sort_properties`](#sort_properties)
- [`statistics`](#statistics)
- [`test`](#test)
- [`tree_node`](#tree_node) - Tree node implementation for Logical Expressions
- [`type_coercion`](#type_coercion) - Type coercion rules for DataFusion
- [`udf_eq`](#udf_eq)
- [`utils`](#utils) - Expression utilities
- [`var_provider`](#var_provider) - Variable provider
- [`window_frame`](#window_frame) - Window frame module
- [`window_state`](#window_state) - Structures used to hold window function state (for implementing WindowUDFs)

**Macros**

- [`expr_vec_fmt`](#expr_vec_fmt)

---

## Module: arguments

Argument resolution logic for named function parameters



## Module: async_udf



## Module: conditional_expressions

Conditional expressions



## Module: dml

DML (Data Manipulation Language) types for DELETE, UPDATE operations.



## Module: execution_props



## Module: expr

Logical Expressions: [`Expr`]



## Module: expr_fn

Functions for creating logical expressions



## Module: expr_rewriter

Expression rewriter



## Module: expr_schema



## datafusion_expr::expr_vec_fmt

*Declarative Macro*

```rust
macro_rules! expr_vec_fmt {
    ( $ARRAY:expr ) => { ... };
}
```



## Module: function

Function module contains typing and signature for built-in and user defined functions.



## Module: groups_accumulator



## Module: interval_arithmetic



## Module: logical_plan



## Module: planner

[`ContextProvider`] and [`ExprPlanner`] APIs to customize SQL query planning



## Module: ptr_eq



## Module: registry

FunctionRegistry trait



## Module: select_expr



## Module: simplify

Structs and traits to provide the information needed for expression simplification.



## Module: sort_properties



## Module: statistics



## Module: test



## Module: tree_node

Tree node implementation for Logical Expressions



## Module: type_coercion

Type coercion rules for DataFusion

Coercion is performed automatically by DataFusion when the types
of arguments passed to a function or needed by operators do not
exactly match the types required by that function / operator. In
this case, DataFusion will attempt to *coerce* the arguments to
types accepted by the function by inserting CAST operations.

CAST operations added by coercion are lossless and never discard
information.

For example coercion from i32 -> i64 might be
performed because all valid i32 values can be represented using an
i64. However, i64 -> i32 is never performed as there are i64
values which can not be represented by i32 values.



## Module: udf_eq



## Module: utils

Expression utilities



## Module: var_provider

Variable provider



## Module: window_frame

Window frame module

The frame-spec determines which output rows are read by an aggregate window function. The frame-spec consists of four parts:
- A frame type - either ROWS, RANGE or GROUPS,
- A starting frame boundary,
- An ending frame boundary,
- An EXCLUDE clause.



## Module: window_state

Structures used to hold window function state (for implementing WindowUDFs)



