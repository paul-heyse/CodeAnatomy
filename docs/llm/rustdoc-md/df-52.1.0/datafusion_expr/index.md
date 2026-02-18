# datafusion_expr

[DataFusion](https://github.com/apache/datafusion)
is an extensible query execution framework that uses
[Apache Arrow](https://arrow.apache.org) as its in-memory format.

This crate is a submodule of DataFusion that provides types representing
logical query plans ([LogicalPlan]) and logical expressions ([Expr]) as well as utilities for
working with these types.

The [expr_fn] module contains functions for creating expressions.

## Modules

### [`datafusion_expr`](datafusion_expr.md)

*1 macro, 28 modules*

### [`arguments`](arguments.md)

*1 function, 1 struct*

### [`async_udf`](async_udf.md)

*1 struct, 1 trait*

### [`conditional_expressions`](conditional_expressions.md)

*1 struct*

### [`execution_props`](execution_props.md)

*1 struct*

### [`expr`](expr.md)

*1 type alias, 2 constants, 21 structs, 4 functions, 5 enums*

### [`expr_fn`](expr_fn.md)

*1 enum, 1 trait, 4 structs, 46 functions*

### [`expr_rewriter`](expr_rewriter.md)

*1 enum, 1 struct, 1 trait, 11 functions*

### [`expr_rewriter::guarantees`](expr_rewriter/guarantees.md)

*1 struct, 2 functions*

### [`expr_rewriter::order_by`](expr_rewriter/order_by.md)

*1 function*

### [`expr_schema`](expr_schema.md)

*1 function, 1 trait*

### [`function`](function.md)

*1 enum, 6 type aliases*

### [`literal`](literal.md)

*2 traits, 3 functions*

### [`logical_plan`](logical_plan.md)

*4 modules*

### [`logical_plan::builder`](logical_plan/builder.md)

*1 constant, 18 functions, 3 structs*

### [`logical_plan::ddl`](logical_plan/ddl.md)

*1 enum, 14 structs*

### [`logical_plan::display`](logical_plan/display.md)

*1 function, 3 structs*

### [`logical_plan::dml`](logical_plan/dml.md)

*2 enums, 2 structs*

### [`logical_plan::extension`](logical_plan/extension.md)

*2 traits*

### [`logical_plan::invariants`](logical_plan/invariants.md)

*1 enum, 2 functions*

### [`logical_plan::plan`](logical_plan/plan.md)

*1 function, 23 structs, 5 enums*

### [`logical_plan::statement`](logical_plan/statement.md)

*4 enums, 7 structs*

### [`partition_evaluator`](partition_evaluator.md)

*1 trait*

### [`planner`](planner.md)

*2 enums, 5 traits, 6 structs*

### [`ptr_eq`](ptr_eq.md)

*1 struct, 2 functions*

### [`registry`](registry.md)

*1 struct, 2 traits*

### [`select_expr`](select_expr.md)

*1 enum*

### [`simplify`](simplify.md)

*1 enum, 1 struct, 1 trait*

### [`table_source`](table_source.md)

*1 trait, 2 enums*

### [`test`](test.md)

*1 module*

### [`test::function_stub`](test/function_stub.md)

*10 functions, 5 structs*

### [`type_coercion`](type_coercion.md)

*3 modules, 7 functions*

### [`type_coercion::functions`](type_coercion/functions.md)

*1 trait, 6 functions*

### [`type_coercion::other`](type_coercion/other.md)

*2 functions*

### [`udaf`](udaf.md)

*1 trait, 2 enums, 2 structs, 6 functions*

### [`udf`](udf.md)

*1 trait, 3 structs*

### [`udf_eq`](udf_eq.md)

*1 struct*

### [`udwf`](udwf.md)

*1 struct, 1 trait, 2 enums*

### [`utils`](utils.md)

*36 functions*

### [`var_provider`](var_provider.md)

*1 enum, 1 function, 1 trait*

### [`window_frame`](window_frame.md)

*1 struct, 2 enums*

### [`window_state`](window_state.md)

*1 enum, 4 structs*

