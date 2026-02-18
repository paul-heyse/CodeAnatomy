**datafusion_expr > expr_fn**

# Module: expr_fn

## Contents

**Structs**

- [`ExprFuncBuilder`](#exprfuncbuilder) - Implementation of [`ExprFunctionExt`].
- [`SimpleAggregateUDF`](#simpleaggregateudf) - Implements [`AggregateUDFImpl`] for functions that have a single signature and
- [`SimpleScalarUDF`](#simplescalarudf) - Implements [`ScalarUDFImpl`] for functions that have a single signature and
- [`SimpleWindowUDF`](#simplewindowudf) - Implements [`WindowUDFImpl`] for functions that have a single signature and

**Enums**

- [`ExprFuncKind`](#exprfunckind)

**Functions**

- [`and`](#and) - Return a new expression with a logical AND
- [`binary_expr`](#binary_expr) - Return a new expression `left <op> right`
- [`bitwise_and`](#bitwise_and) - Return a new expression with bitwise AND
- [`bitwise_or`](#bitwise_or) - Return a new expression with bitwise OR
- [`bitwise_shift_left`](#bitwise_shift_left) - Return a new expression with bitwise SHIFT LEFT
- [`bitwise_shift_right`](#bitwise_shift_right) - Return a new expression with bitwise SHIFT RIGHT
- [`bitwise_xor`](#bitwise_xor) - Return a new expression with bitwise XOR
- [`case`](#case) - Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
- [`cast`](#cast) - Create a cast expression
- [`col`](#col) - Create a column expression based on a qualified or unqualified column name. Will
- [`create_udaf`](#create_udaf) - Creates a new UDAF with a specific signature, state type and return type.
- [`create_udf`](#create_udf) - Convenience method to create a new user defined scalar function (UDF) with a
- [`create_udwf`](#create_udwf) - Creates a new UDWF with a specific signature, state type and return type.
- [`cube`](#cube) - Create a grouping set for all combination of `exprs`
- [`exists`](#exists) - Create an EXISTS subquery expression
- [`grouping_set`](#grouping_set) - Create a grouping set
- [`ident`](#ident) - Create an unqualified column expression from the provided name, without normalizing
- [`in_list`](#in_list) - Create an in_list expression
- [`in_subquery`](#in_subquery) - Create an IN subquery expression
- [`interval_datetime_lit`](#interval_datetime_lit)
- [`interval_month_day_nano_lit`](#interval_month_day_nano_lit)
- [`interval_year_month_lit`](#interval_year_month_lit)
- [`is_false`](#is_false) - Create is false expression
- [`is_not_false`](#is_not_false) - Create is not false expression
- [`is_not_null`](#is_not_null) - Create is not null expression
- [`is_not_true`](#is_not_true) - Create is not true expression
- [`is_not_unknown`](#is_not_unknown) - Create is not unknown expression
- [`is_null`](#is_null) - Create is null expression
- [`is_true`](#is_true) - Create is true expression
- [`is_unknown`](#is_unknown) - Create is unknown expression
- [`not`](#not) - Return a new expression with a logical NOT
- [`not_exists`](#not_exists) - Create a NOT EXISTS subquery expression
- [`not_in_subquery`](#not_in_subquery) - Create a NOT IN subquery expression
- [`or`](#or) - Return a new expression with a logical OR
- [`out_ref_col`](#out_ref_col) - Create an out reference column which hold a reference that has been resolved to a field
- [`out_ref_col_with_metadata`](#out_ref_col_with_metadata) - Create an out reference column from an existing field (preserving metadata)
- [`placeholder`](#placeholder) - Create placeholder value that will be filled in (such as `$1`)
- [`qualified_wildcard`](#qualified_wildcard) - Create an 't.*' [`Expr::Wildcard`] expression that matches all columns from a specific table
- [`qualified_wildcard_with_options`](#qualified_wildcard_with_options) - Create an 't.*' [`Expr::Wildcard`] expression with the wildcard options
- [`rollup`](#rollup) - Create a grouping set for rollup
- [`scalar_subquery`](#scalar_subquery) - Create a scalar subquery expression
- [`try_cast`](#try_cast) - Create a try cast expression
- [`unnest`](#unnest) - Create a Unnest expression
- [`when`](#when) - Create a CASE WHEN statement with boolean WHEN expressions and no base expression.
- [`wildcard`](#wildcard) - Create an '*' [`Expr::Wildcard`] expression that matches all columns
- [`wildcard_with_options`](#wildcard_with_options) - Create an '*' [`Expr::Wildcard`] expression with the wildcard options

**Traits**

- [`ExprFunctionExt`](#exprfunctionext) - Extensions for configuring [`Expr::AggregateFunction`] or [`Expr::WindowFunction`]

---

## datafusion_expr::expr_fn::ExprFuncBuilder

*Struct*

Implementation of [`ExprFunctionExt`].

See [`ExprFunctionExt`] for usage and examples

**Methods:**

- `fn build(self: Self) -> Result<Expr>` - Updates and returns the in progress [`Expr::AggregateFunction`] or [`Expr::WindowFunction`]

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ExprFunctionExt**
  - `fn order_by(self: Self, order_by: Vec<Sort>) -> ExprFuncBuilder` - Add `ORDER BY <order_by>`
  - `fn filter(self: Self, filter: Expr) -> ExprFuncBuilder` - Add `FILTER <filter>`
  - `fn distinct(self: Self) -> ExprFuncBuilder` - Add `DISTINCT`
  - `fn null_treatment<impl Into<Option<NullTreatment>>>(self: Self, null_treatment: impl Trait) -> ExprFuncBuilder` - Add `RESPECT NULLS` or `IGNORE NULLS`
  - `fn partition_by(self: Self, partition_by: Vec<Expr>) -> ExprFuncBuilder`
  - `fn window_frame(self: Self, window_frame: WindowFrame) -> ExprFuncBuilder`
- **Clone**
  - `fn clone(self: &Self) -> ExprFuncBuilder`



## datafusion_expr::expr_fn::ExprFuncKind

*Enum*

**Variants:**
- `Aggregate(crate::expr::AggregateFunction)`
- `Window(Box<crate::expr::WindowFunction>)`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ExprFuncKind`



## datafusion_expr::expr_fn::ExprFunctionExt

*Trait*

Extensions for configuring [`Expr::AggregateFunction`] or [`Expr::WindowFunction`]

Adds methods to [`Expr`] that make it easy to set optional options
such as `ORDER BY`, `FILTER` and `DISTINCT`

# Example
```no_run
# use datafusion_common::Result;
# use datafusion_expr::expr::NullTreatment;
# use datafusion_expr::test::function_stub::count;
# use datafusion_expr::{ExprFunctionExt, lit, Expr, col};
# // first_value is an aggregate function in another crate
# fn first_value(_arg: Expr) -> Expr {
unimplemented!() }
# fn main() -> Result<()> {
// Create an aggregate count, filtering on column y > 5
let agg = count(col("x")).filter(col("y").gt(lit(5))).build()?;

// Find the first value in an aggregate sorted by column y
// equivalent to:
// `FIRST_VALUE(x ORDER BY y ASC IGNORE NULLS)`
let sort_expr = col("y").sort(true, true);
let agg = first_value(col("x"))
    .order_by(vec![sort_expr])
    .null_treatment(NullTreatment::IgnoreNulls)
    .build()?;

// Create a window expression for percent rank partitioned on column a
// equivalent to:
// `PERCENT_RANK() OVER (PARTITION BY a ORDER BY b ASC NULLS LAST IGNORE NULLS)`
// percent_rank is an udwf function in another crate
# fn percent_rank() -> Expr {
unimplemented!() }
let window = percent_rank()
    .partition_by(vec![col("a")])
    .order_by(vec![col("b").sort(true, true)])
    .null_treatment(NullTreatment::IgnoreNulls)
    .build()?;
#     Ok(())
# }
```

**Methods:**

- `order_by`: Add `ORDER BY <order_by>`
- `filter`: Add `FILTER <filter>`
- `distinct`: Add `DISTINCT`
- `null_treatment`: Add `RESPECT NULLS` or `IGNORE NULLS`
- `partition_by`: Add `PARTITION BY`
- `window_frame`: Add appropriate window frame conditions



## datafusion_expr::expr_fn::SimpleAggregateUDF

*Struct*

Implements [`AggregateUDFImpl`] for functions that have a single signature and
return type.

**Methods:**

- `fn new<impl Into<String>>(name: impl Trait, input_type: Vec<DataType>, return_type: DataType, volatility: Volatility, accumulator: AccumulatorFactoryFunction, state_fields: Vec<FieldRef>) -> Self` - Create a new `SimpleAggregateUDF` from a name, input types, return type, state type and
- `fn new_with_signature<impl Into<String>>(name: impl Trait, signature: Signature, return_type: DataType, accumulator: AccumulatorFactoryFunction, state_fields: Vec<FieldRef>) -> Self` - Create a new `SimpleAggregateUDF` from a name, signature, return type, state type and

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &SimpleAggregateUDF) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **AggregateUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn accumulator(self: &Self, acc_args: AccumulatorArgs) -> Result<Box<dyn crate::Accumulator>>`
  - `fn state_fields(self: &Self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>>`



## datafusion_expr::expr_fn::SimpleScalarUDF

*Struct*

Implements [`ScalarUDFImpl`] for functions that have a single signature and
return type.

**Methods:**

- `fn new<impl Into<String>>(name: impl Trait, input_types: Vec<DataType>, return_type: DataType, volatility: Volatility, fun: ScalarFunctionImplementation) -> Self` - Create a new `SimpleScalarUDF` from a name, input types, return type and
- `fn new_with_signature<impl Into<String>>(name: impl Trait, signature: Signature, return_type: DataType, fun: ScalarFunctionImplementation) -> Self` - Create a new `SimpleScalarUDF` from a name, signature, return type and

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn invoke_with_args(self: &Self, args: ScalarFunctionArgs) -> Result<ColumnarValue>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &SimpleScalarUDF) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::expr_fn::SimpleWindowUDF

*Struct*

Implements [`WindowUDFImpl`] for functions that have a single signature and
return type.

**Methods:**

- `fn new<impl Into<String>>(name: impl Trait, input_type: DataType, return_type: DataType, volatility: Volatility, partition_evaluator_factory: PartitionEvaluatorFactory) -> Self` - Create a new `SimpleWindowUDF` from a name, input types, return type and

**Traits:** Eq

**Trait Implementations:**

- **WindowUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn partition_evaluator(self: &Self, _partition_evaluator_args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>>`
  - `fn field(self: &Self, field_args: WindowUDFFieldArgs) -> Result<FieldRef>`
  - `fn limit_effect(self: &Self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect`
- **PartialEq**
  - `fn eq(self: &Self, other: &SimpleWindowUDF) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## datafusion_expr::expr_fn::and

*Function*

Return a new expression with a logical AND

```rust
fn and(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::binary_expr

*Function*

Return a new expression `left <op> right`

```rust
fn binary_expr(left: crate::Expr, op: crate::Operator, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::bitwise_and

*Function*

Return a new expression with bitwise AND

```rust
fn bitwise_and(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::bitwise_or

*Function*

Return a new expression with bitwise OR

```rust
fn bitwise_or(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::bitwise_shift_left

*Function*

Return a new expression with bitwise SHIFT LEFT

```rust
fn bitwise_shift_left(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::bitwise_shift_right

*Function*

Return a new expression with bitwise SHIFT RIGHT

```rust
fn bitwise_shift_right(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::bitwise_xor

*Function*

Return a new expression with bitwise XOR

```rust
fn bitwise_xor(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::case

*Function*

Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.

```rust
fn case(expr: crate::Expr) -> crate::conditional_expressions::CaseBuilder
```



## datafusion_expr::expr_fn::cast

*Function*

Create a cast expression

```rust
fn cast(expr: crate::Expr, data_type: arrow::datatypes::DataType) -> crate::Expr
```



## datafusion_expr::expr_fn::col

*Function*

Create a column expression based on a qualified or unqualified column name. Will
normalize unquoted identifiers according to SQL rules (identifiers will become lowercase).

For example:

```rust
# use datafusion_expr::col;
let c1 = col("a");
let c2 = col("A");
assert_eq!(c1, c2);

// note how quoting with double quotes preserves the case
let c3 = col(r#""A""#);
assert_ne!(c1, c3);
```

```rust
fn col<impl Into<Column>>(ident: impl Trait) -> crate::Expr
```



## datafusion_expr::expr_fn::create_udaf

*Function*

Creates a new UDAF with a specific signature, state type and return type.
The signature and state type must match the `Accumulator's implementation`.

```rust
fn create_udaf(name: &str, input_type: Vec<arrow::datatypes::DataType>, return_type: std::sync::Arc<arrow::datatypes::DataType>, volatility: crate::Volatility, accumulator: crate::function::AccumulatorFactoryFunction, state_type: std::sync::Arc<Vec<arrow::datatypes::DataType>>) -> crate::AggregateUDF
```



## datafusion_expr::expr_fn::create_udf

*Function*

Convenience method to create a new user defined scalar function (UDF) with a
specific signature and specific return type.

Note this function does not expose all available features of [`ScalarUDF`],
such as

* computing return types based on input types
* multiple [`Signature`]s
* aliases

See [`ScalarUDF`] for details and examples on how to use the full
functionality.

```rust
fn create_udf(name: &str, input_types: Vec<arrow::datatypes::DataType>, return_type: arrow::datatypes::DataType, volatility: crate::Volatility, fun: crate::ScalarFunctionImplementation) -> crate::ScalarUDF
```



## datafusion_expr::expr_fn::create_udwf

*Function*

Creates a new UDWF with a specific signature, state type and return type.

The signature and state type must match the [`PartitionEvaluator`]'s implementation`.

[`PartitionEvaluator`]: crate::PartitionEvaluator

```rust
fn create_udwf(name: &str, input_type: arrow::datatypes::DataType, return_type: std::sync::Arc<arrow::datatypes::DataType>, volatility: crate::Volatility, partition_evaluator_factory: crate::function::PartitionEvaluatorFactory) -> crate::WindowUDF
```



## datafusion_expr::expr_fn::cube

*Function*

Create a grouping set for all combination of `exprs`

```rust
fn cube(exprs: Vec<crate::Expr>) -> crate::Expr
```



## datafusion_expr::expr_fn::exists

*Function*

Create an EXISTS subquery expression

```rust
fn exists(subquery: std::sync::Arc<crate::LogicalPlan>) -> crate::Expr
```



## datafusion_expr::expr_fn::grouping_set

*Function*

Create a grouping set

```rust
fn grouping_set(exprs: Vec<Vec<crate::Expr>>) -> crate::Expr
```



## datafusion_expr::expr_fn::ident

*Function*

Create an unqualified column expression from the provided name, without normalizing
the column.

For example:

```rust
# use datafusion_expr::{col, ident};
let c1 = ident("A"); // not normalized staying as column 'A'
let c2 = col("A"); // normalized via SQL rules becoming column 'a'
assert_ne!(c1, c2);

let c3 = col(r#""A""#);
assert_eq!(c1, c3);

let c4 = col("t1.a"); // parses as relation 't1' column 'a'
let c5 = ident("t1.a"); // parses as column 't1.a'
assert_ne!(c4, c5);
```

```rust
fn ident<impl Into<String>>(name: impl Trait) -> crate::Expr
```



## datafusion_expr::expr_fn::in_list

*Function*

Create an in_list expression

```rust
fn in_list(expr: crate::Expr, list: Vec<crate::Expr>, negated: bool) -> crate::Expr
```



## datafusion_expr::expr_fn::in_subquery

*Function*

Create an IN subquery expression

```rust
fn in_subquery(expr: crate::Expr, subquery: std::sync::Arc<crate::LogicalPlan>) -> crate::Expr
```



## datafusion_expr::expr_fn::interval_datetime_lit

*Function*

```rust
fn interval_datetime_lit(value: &str) -> crate::Expr
```



## datafusion_expr::expr_fn::interval_month_day_nano_lit

*Function*

```rust
fn interval_month_day_nano_lit(value: &str) -> crate::Expr
```



## datafusion_expr::expr_fn::interval_year_month_lit

*Function*

```rust
fn interval_year_month_lit(value: &str) -> crate::Expr
```



## datafusion_expr::expr_fn::is_false

*Function*

Create is false expression

```rust
fn is_false(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_not_false

*Function*

Create is not false expression

```rust
fn is_not_false(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_not_null

*Function*

Create is not null expression

```rust
fn is_not_null(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_not_true

*Function*

Create is not true expression

```rust
fn is_not_true(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_not_unknown

*Function*

Create is not unknown expression

```rust
fn is_not_unknown(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_null

*Function*

Create is null expression

```rust
fn is_null(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_true

*Function*

Create is true expression

```rust
fn is_true(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::is_unknown

*Function*

Create is unknown expression

```rust
fn is_unknown(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::not

*Function*

Return a new expression with a logical NOT

```rust
fn not(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::not_exists

*Function*

Create a NOT EXISTS subquery expression

```rust
fn not_exists(subquery: std::sync::Arc<crate::LogicalPlan>) -> crate::Expr
```



## datafusion_expr::expr_fn::not_in_subquery

*Function*

Create a NOT IN subquery expression

```rust
fn not_in_subquery(expr: crate::Expr, subquery: std::sync::Arc<crate::LogicalPlan>) -> crate::Expr
```



## datafusion_expr::expr_fn::or

*Function*

Return a new expression with a logical OR

```rust
fn or(left: crate::Expr, right: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::out_ref_col

*Function*

Create an out reference column which hold a reference that has been resolved to a field
outside of the current plan.
The expression created by this function does not preserve the metadata of the outer column.
Please use `out_ref_col_with_metadata` if you want to preserve the metadata.

```rust
fn out_ref_col<impl Into<Column>>(dt: arrow::datatypes::DataType, ident: impl Trait) -> crate::Expr
```



## datafusion_expr::expr_fn::out_ref_col_with_metadata

*Function*

Create an out reference column from an existing field (preserving metadata)

```rust
fn out_ref_col_with_metadata<impl Into<Column>>(dt: arrow::datatypes::DataType, metadata: std::collections::HashMap<String, String>, ident: impl Trait) -> crate::Expr
```



## datafusion_expr::expr_fn::placeholder

*Function*

Create placeholder value that will be filled in (such as `$1`)

Note the parameter type can be inferred using [`Expr::infer_placeholder_types`]

# Example

```rust
# use datafusion_expr::{placeholder};
let p = placeholder("$1"); // $1, refers to parameter 1
assert_eq!(p.to_string(), "$1")
```

```rust
fn placeholder<impl Into<String>>(id: impl Trait) -> crate::Expr
```



## datafusion_expr::expr_fn::qualified_wildcard

*Function*

Create an 't.*' [`Expr::Wildcard`] expression that matches all columns from a specific table

# Example

```rust
# use datafusion_common::TableReference;
# use datafusion_expr::{qualified_wildcard};
let p = qualified_wildcard(TableReference::bare("t"));
assert_eq!(p.to_string(), "t.*")
```

```rust
fn qualified_wildcard<impl Into<TableReference>>(qualifier: impl Trait) -> crate::select_expr::SelectExpr
```



## datafusion_expr::expr_fn::qualified_wildcard_with_options

*Function*

Create an 't.*' [`Expr::Wildcard`] expression with the wildcard options

```rust
fn qualified_wildcard_with_options<impl Into<TableReference>>(qualifier: impl Trait, options: crate::expr::WildcardOptions) -> crate::select_expr::SelectExpr
```



## datafusion_expr::expr_fn::rollup

*Function*

Create a grouping set for rollup

```rust
fn rollup(exprs: Vec<crate::Expr>) -> crate::Expr
```



## datafusion_expr::expr_fn::scalar_subquery

*Function*

Create a scalar subquery expression

```rust
fn scalar_subquery(subquery: std::sync::Arc<crate::LogicalPlan>) -> crate::Expr
```



## datafusion_expr::expr_fn::try_cast

*Function*

Create a try cast expression

```rust
fn try_cast(expr: crate::Expr, data_type: arrow::datatypes::DataType) -> crate::Expr
```



## datafusion_expr::expr_fn::unnest

*Function*

Create a Unnest expression

```rust
fn unnest(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_fn::when

*Function*

Create a CASE WHEN statement with boolean WHEN expressions and no base expression.

```rust
fn when(when: crate::Expr, then: crate::Expr) -> crate::conditional_expressions::CaseBuilder
```



## datafusion_expr::expr_fn::wildcard

*Function*

Create an '*' [`Expr::Wildcard`] expression that matches all columns

# Example

```rust
# use datafusion_expr::{wildcard};
let p = wildcard();
assert_eq!(p.to_string(), "*")
```

```rust
fn wildcard() -> crate::select_expr::SelectExpr
```



## datafusion_expr::expr_fn::wildcard_with_options

*Function*

Create an '*' [`Expr::Wildcard`] expression with the wildcard options

```rust
fn wildcard_with_options(options: crate::expr::WildcardOptions) -> crate::select_expr::SelectExpr
```



