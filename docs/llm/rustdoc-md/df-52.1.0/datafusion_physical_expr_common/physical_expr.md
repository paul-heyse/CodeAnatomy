**datafusion_physical_expr_common > physical_expr**

# Module: physical_expr

## Contents

**Functions**

- [`fmt_sql`](#fmt_sql) - Prints a [`PhysicalExpr`] in a SQL-like format
- [`format_physical_expr_list`](#format_physical_expr_list) - Returns [`Display`] able a list of [`PhysicalExpr`]
- [`is_dynamic_physical_expr`](#is_dynamic_physical_expr) - Check if the given `PhysicalExpr` is dynamic.
- [`is_volatile`](#is_volatile) - Returns true if the expression is volatile, i.e. whether it can return different
- [`snapshot_generation`](#snapshot_generation) - Check the generation of this `PhysicalExpr`.
- [`snapshot_physical_expr`](#snapshot_physical_expr) - Take a snapshot of the given `PhysicalExpr` if it is dynamic.
- [`snapshot_physical_expr_opt`](#snapshot_physical_expr_opt) - Take a snapshot of the given `PhysicalExpr` if it is dynamic.
- [`with_new_children_if_necessary`](#with_new_children_if_necessary) - Returns a copy of this expr if we change any child according to the pointer comparison.

**Traits**

- [`PhysicalExpr`](#physicalexpr) - [`PhysicalExpr`]s represent expressions such as `A + 1` or `CAST(c1 AS int)`.

**Type Aliases**

- [`PhysicalExprRef`](#physicalexprref) - Shared [`PhysicalExpr`].

---

## datafusion_physical_expr_common::physical_expr::PhysicalExpr

*Trait*

[`PhysicalExpr`]s represent expressions such as `A + 1` or `CAST(c1 AS int)`.

`PhysicalExpr` knows its type, nullability and can be evaluated directly on
a [`RecordBatch`] (see [`Self::evaluate`]).

`PhysicalExpr` are the physical counterpart to [`Expr`] used in logical
planning. They are typically created from [`Expr`] by a [`PhysicalPlanner`]
invoked from a higher level API

Some important examples of `PhysicalExpr` are:
* [`Column`]: Represents a column at a given index in a RecordBatch

To create `PhysicalExpr` from  `Expr`, see
* [`SessionContext::create_physical_expr`]: A high level API
* [`create_physical_expr`]: A low level API

# Formatting `PhysicalExpr` as strings
There are three ways to format `PhysicalExpr` as a string:
* [`Debug`]: Standard Rust debugging format (e.g. `Constant { value: ... }`)
* [`Display`]: Detailed SQL-like format that shows expression structure (e.g. (`Utf8 ("foobar")`). This is often used for debugging and tests
* [`Self::fmt_sql`]: SQL-like human readable format (e.g. ('foobar')`), See also [`sql_fmt`]

[`SessionContext::create_physical_expr`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.create_physical_expr
[`PhysicalPlanner`]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html
[`Expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`create_physical_expr`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/fn.create_physical_expr.html
[`Column`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/expressions/struct.Column.html

**Methods:**

- `as_any`: Returns the physical expression as [`Any`] so that it can be
- `data_type`: Get the data type of this expression, given the schema of the input
- `nullable`: Determine whether this expression is nullable, given the schema of the input
- `evaluate`: Evaluate an expression against a RecordBatch
- `return_field`: The output field associated with this expression
- `evaluate_selection`: Evaluate an expression against a RecordBatch after first applying a validity array
- `children`: Get a list of child PhysicalExpr that provide the input for this expr.
- `with_new_children`: Returns a new PhysicalExpr where all children were replaced by new exprs.
- `evaluate_bounds`: Computes the output interval for the expression, given the input
- `propagate_constraints`: Updates bounds for child expressions, given a known interval for this
- `evaluate_statistics`: Computes the output statistics for the expression, given the input
- `propagate_statistics`: Updates children statistics using the given parent statistic for this
- `get_properties`: Calculates the properties of this [`PhysicalExpr`] based on its
- `fmt_sql`: Format this `PhysicalExpr` in nice human readable "SQL" format
- `snapshot`: Take a snapshot of this `PhysicalExpr`, if it is dynamic.
- `snapshot_generation`: Returns the generation of this `PhysicalExpr` for snapshotting purposes.
- `is_volatile_node`: Returns true if the expression node is volatile, i.e. whether it can return



## datafusion_physical_expr_common::physical_expr::PhysicalExprRef

*Type Alias*: `std::sync::Arc<dyn PhysicalExpr>`

Shared [`PhysicalExpr`].



## datafusion_physical_expr_common::physical_expr::fmt_sql

*Function*

Prints a [`PhysicalExpr`] in a SQL-like format

# Example
```
# // The boilerplate needed to create a `PhysicalExpr` for the example
# use std::any::Any;
use std::collections::HashMap;
# use std::fmt::Formatter;
# use std::sync::Arc;
# use arrow::array::RecordBatch;
# use arrow::datatypes::{DataType, Field, FieldRef, Schema};
# use datafusion_common::Result;
# use datafusion_expr_common::columnar_value::ColumnarValue;
# use datafusion_physical_expr_common::physical_expr::{fmt_sql, DynEq, PhysicalExpr};
# #[derive(Debug, PartialEq, Eq, Hash)]
# struct MyExpr {}
# impl PhysicalExpr for MyExpr {fn as_any(&self) -> &dyn Any { unimplemented!() }
# fn data_type(&self, input_schema: &Schema) -> Result<DataType> { unimplemented!() }
# fn nullable(&self, input_schema: &Schema) -> Result<bool> { unimplemented!() }
# fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> { unimplemented!() }
# fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> { unimplemented!() }
# fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>{ unimplemented!() }
# fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> { unimplemented!() }
# fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "CASE a > b THEN 1 ELSE 0 END") }
# }
# impl std::fmt::Display for MyExpr {fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { unimplemented!() } }
# fn make_physical_expr() -> Arc<dyn PhysicalExpr> { Arc::new(MyExpr{}) }
let expr: Arc<dyn PhysicalExpr> = make_physical_expr();
// wrap the expression in `sql_fmt` which can be used with
// `format!`, `to_string()`, etc
let expr_as_sql = fmt_sql(expr.as_ref());
assert_eq!(
  "The SQL: CASE a > b THEN 1 ELSE 0 END",
  format!("The SQL: {expr_as_sql}")
);
```

```rust
fn fmt_sql(expr: &dyn PhysicalExpr) -> impl Trait
```



## datafusion_physical_expr_common::physical_expr::format_physical_expr_list

*Function*

Returns [`Display`] able a list of [`PhysicalExpr`]

Example output: `[a + 1, b]`

```rust
fn format_physical_expr_list<T>(exprs: T) -> impl Trait
```



## datafusion_physical_expr_common::physical_expr::is_dynamic_physical_expr

*Function*

Check if the given `PhysicalExpr` is dynamic.
Internally this calls [`snapshot_generation`] to check if the generation is non-zero,
any dynamic `PhysicalExpr` should have a non-zero generation.

```rust
fn is_dynamic_physical_expr(expr: &std::sync::Arc<dyn PhysicalExpr>) -> bool
```



## datafusion_physical_expr_common::physical_expr::is_volatile

*Function*

Returns true if the expression is volatile, i.e. whether it can return different
results when evaluated multiple times with the same input.

For example the function call `RANDOM()` is volatile as each call will
return a different value.

This method recursively checks if any sub-expression is volatile, for example
`1 + RANDOM()` will return `true`.

```rust
fn is_volatile(expr: &std::sync::Arc<dyn PhysicalExpr>) -> bool
```



## datafusion_physical_expr_common::physical_expr::snapshot_generation

*Function*

Check the generation of this `PhysicalExpr`.
Dynamic `PhysicalExpr`s may have a generation that is incremented
every time the state of the `PhysicalExpr` changes.
If the generation changes that means this `PhysicalExpr` or one of its children
has changed since the last time it was evaluated.

This algorithm will not produce collisions as long as the structure of the
`PhysicalExpr` does not change and no `PhysicalExpr` decrements its own generation.

```rust
fn snapshot_generation(expr: &std::sync::Arc<dyn PhysicalExpr>) -> u64
```



## datafusion_physical_expr_common::physical_expr::snapshot_physical_expr

*Function*

Take a snapshot of the given `PhysicalExpr` if it is dynamic.

Take a snapshot of this `PhysicalExpr` if it is dynamic.
This is used to capture the current state of `PhysicalExpr`s that may contain
dynamic references to other operators in order to serialize it over the wire
or treat it via downcast matching.

See the documentation of [`PhysicalExpr::snapshot`] for more details.

# Returns

Returns a snapshot of the `PhysicalExpr` if it is dynamic, otherwise
returns itself.

```rust
fn snapshot_physical_expr(expr: std::sync::Arc<dyn PhysicalExpr>) -> datafusion_common::Result<std::sync::Arc<dyn PhysicalExpr>>
```



## datafusion_physical_expr_common::physical_expr::snapshot_physical_expr_opt

*Function*

Take a snapshot of the given `PhysicalExpr` if it is dynamic.

Take a snapshot of this `PhysicalExpr` if it is dynamic.
This is used to capture the current state of `PhysicalExpr`s that may contain
dynamic references to other operators in order to serialize it over the wire
or treat it via downcast matching.

See the documentation of [`PhysicalExpr::snapshot`] for more details.

# Returns

Returns a `[`Transformed`] indicating whether a snapshot was taken,
along with the resulting `PhysicalExpr`.

```rust
fn snapshot_physical_expr_opt(expr: std::sync::Arc<dyn PhysicalExpr>) -> datafusion_common::Result<datafusion_common::tree_node::Transformed<std::sync::Arc<dyn PhysicalExpr>>>
```



## datafusion_physical_expr_common::physical_expr::with_new_children_if_necessary

*Function*

Returns a copy of this expr if we change any child according to the pointer comparison.
The size of `children` must be equal to the size of `PhysicalExpr::children()`.

```rust
fn with_new_children_if_necessary(expr: std::sync::Arc<dyn PhysicalExpr>, children: Vec<std::sync::Arc<dyn PhysicalExpr>>) -> datafusion_common::Result<std::sync::Arc<dyn PhysicalExpr>>
```



