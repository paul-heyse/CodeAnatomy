**datafusion_expr > utils**

# Module: utils

## Contents

**Functions**

- [`add_filter`](#add_filter) - Returns a new [LogicalPlan] that filters the output of  `plan` with a
- [`can_hash`](#can_hash) - Can this data type be used in hash join equal conditions??
- [`check_all_columns_from_schema`](#check_all_columns_from_schema) - Check whether all columns are from the schema.
- [`collect_subquery_cols`](#collect_subquery_cols) - Determine the set of [`Column`]s produced by the subquery.
- [`columnize_expr`](#columnize_expr) - Convert an expression into Column expression if it's already provided as input plan.
- [`compare_sort_expr`](#compare_sort_expr) - Compare the sort expr as PostgreSQL's common_prefix_cmp():
- [`conjunction`](#conjunction) - Combines an array of filter expressions into a single filter
- [`disjunction`](#disjunction) - Combines an array of filter expressions into a single filter
- [`enumerate_grouping_sets`](#enumerate_grouping_sets) - Convert multiple grouping expressions into one [`GroupingSet::GroupingSets`],\
- [`expand_qualified_wildcard`](#expand_qualified_wildcard) - Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.
- [`expand_wildcard`](#expand_wildcard) - Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
- [`expr_as_column_expr`](#expr_as_column_expr) - Convert any `Expr` to an `Expr::Column`.
- [`expr_to_columns`](#expr_to_columns) - Recursively walk an expression tree, collecting the unique set of columns
- [`exprlist_to_fields`](#exprlist_to_fields) - Create schema fields from an expression list, for use in result set schema construction
- [`find_aggregate_exprs`](#find_aggregate_exprs) - Collect all deeply nested `Expr::AggregateFunction`.
- [`find_column_exprs`](#find_column_exprs) - Collect all deeply nested `Expr::Column`'s. They are returned in order of
- [`find_join_exprs`](#find_join_exprs) - Looks for correlating expressions: for example, a binary expression with one field from the subquery, and
- [`find_out_reference_exprs`](#find_out_reference_exprs) - Collect all deeply nested `Expr::OuterReferenceColumn`. They are returned in order of occurrence
- [`find_valid_equijoin_key_pair`](#find_valid_equijoin_key_pair) - Give two sides of the equijoin predicate, return a valid join key pair.
- [`find_window_exprs`](#find_window_exprs) - Collect all deeply nested `Expr::WindowFunction`. They are returned in order of occurrence
- [`format_state_name`](#format_state_name) - Build state name. State is the intermediate state of the aggregate function.
- [`generate_signature_error_msg`](#generate_signature_error_msg) - Creates a detailed error message for a function with wrong signature.
- [`generate_sort_key`](#generate_sort_key) - Generate a sort key for a given window expr's partition_by and order_by expr
- [`group_window_expr_by_sort_keys`](#group_window_expr_by_sort_keys) - Group a slice of window expression expr by their order by expressions
- [`grouping_set_expr_count`](#grouping_set_expr_count) - Count the number of distinct exprs in a list of group by expressions. If the
- [`grouping_set_to_exprlist`](#grouping_set_to_exprlist) - Find all distinct exprs in a list of group by expressions. If the
- [`inspect_expr_pre`](#inspect_expr_pre) - Recursively inspect an [`Expr`] and all its children.
- [`iter_conjunction`](#iter_conjunction) - Iterate parts in a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
- [`iter_conjunction_owned`](#iter_conjunction_owned) - Iterate parts in a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
- [`merge_schema`](#merge_schema) - merge inputs schema into a single schema.
- [`only_or_err`](#only_or_err) - Returns the first (and only) element in a slice, or an error
- [`powerset`](#powerset) - The [power set] (or powerset) of a set S is the set of all subsets of S, \
- [`split_binary`](#split_binary) - Splits an binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
- [`split_binary_owned`](#split_binary_owned) - Splits an owned binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
- [`split_conjunction`](#split_conjunction) - Splits a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
- [`split_conjunction_owned`](#split_conjunction_owned) - Splits an owned conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`

---

## datafusion_expr::utils::add_filter

*Function*

Returns a new [LogicalPlan] that filters the output of  `plan` with a
[LogicalPlan::Filter] with all `predicates` ANDed.

# Example
Before:
```text
plan
```

After:
```text
Filter(predicate)
  plan
```

```rust
fn add_filter(plan: crate::LogicalPlan, predicates: &[&crate::Expr]) -> datafusion_common::Result<crate::LogicalPlan>
```



## datafusion_expr::utils::can_hash

*Function*

Can this data type be used in hash join equal conditions??
Data types here come from function 'equal_rows', if more data types are supported
in create_hashes, add those data types here to generate join logical plan.

```rust
fn can_hash(data_type: &arrow::datatypes::DataType) -> bool
```



## datafusion_expr::utils::check_all_columns_from_schema

*Function*

Check whether all columns are from the schema.

```rust
fn check_all_columns_from_schema(columns: &std::collections::HashSet<&datafusion_common::Column>, schema: &datafusion_common::DFSchema) -> datafusion_common::Result<bool>
```



## datafusion_expr::utils::collect_subquery_cols

*Function*

Determine the set of [`Column`]s produced by the subquery.

```rust
fn collect_subquery_cols(exprs: &[crate::Expr], subquery_schema: &datafusion_common::DFSchema) -> datafusion_common::Result<std::collections::BTreeSet<datafusion_common::Column>>
```



## datafusion_expr::utils::columnize_expr

*Function*

Convert an expression into Column expression if it's already provided as input plan.

For example, it rewrites:

```text
.aggregate(vec![col("c1")], vec![sum(col("c2"))])?
.project(vec![col("c1"), sum(col("c2"))?
```

Into:

```text
.aggregate(vec![col("c1")], vec![sum(col("c2"))])?
.project(vec![col("c1"), col("SUM(c2)")?
```

```rust
fn columnize_expr(e: crate::Expr, input: &crate::LogicalPlan) -> datafusion_common::Result<crate::Expr>
```



## datafusion_expr::utils::compare_sort_expr

*Function*

Compare the sort expr as PostgreSQL's common_prefix_cmp():
<https://github.com/postgres/postgres/blob/master/src/backend/optimizer/plan/planner.c>

```rust
fn compare_sort_expr(sort_expr_a: &crate::expr::Sort, sort_expr_b: &crate::expr::Sort, schema: &datafusion_common::DFSchemaRef) -> std::cmp::Ordering
```



## datafusion_expr::utils::conjunction

*Function*

Combines an array of filter expressions into a single filter
expression consisting of the input filter expressions joined with
logical AND.

Returns None if the filters array is empty.

# Example
```
# use datafusion_expr::{col, lit};
# use datafusion_expr::utils::conjunction;
// a=1 AND b=2
let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));

// [a=1, b=2]
let split = vec![col("a").eq(lit(1)), col("b").eq(lit(2))];

// use conjunction to join them together with `AND`
assert_eq!(conjunction(split), Some(expr));
```

```rust
fn conjunction<impl IntoIterator<Item = Expr>>(filters: impl Trait) -> Option<crate::Expr>
```



## datafusion_expr::utils::disjunction

*Function*

Combines an array of filter expressions into a single filter
expression consisting of the input filter expressions joined with
logical OR.

Returns None if the filters array is empty.

# Example
```
# use datafusion_expr::{col, lit};
# use datafusion_expr::utils::disjunction;
// a=1 OR b=2
let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));

// [a=1, b=2]
let split = vec![col("a").eq(lit(1)), col("b").eq(lit(2))];

// use disjunction to join them together with `OR`
assert_eq!(disjunction(split), Some(expr));
```

```rust
fn disjunction<impl IntoIterator<Item = Expr>>(filters: impl Trait) -> Option<crate::Expr>
```



## datafusion_expr::utils::enumerate_grouping_sets

*Function*

Convert multiple grouping expressions into one [`GroupingSet::GroupingSets`],\
if the grouping expression does not contain [`Expr::GroupingSet`] or only has one expression,\
no conversion will be performed.

e.g.

person.id,\
GROUPING SETS ((person.age, person.salary),(person.age)),\
ROLLUP(person.state, person.birth_date)

=>

GROUPING SETS (\
  (person.id, person.age, person.salary),\
  (person.id, person.age, person.salary, person.state),\
  (person.id, person.age, person.salary, person.state, person.birth_date),\
  (person.id, person.age),\
  (person.id, person.age, person.state),\
  (person.id, person.age, person.state, person.birth_date)\
)

```rust
fn enumerate_grouping_sets(group_expr: Vec<crate::Expr>) -> datafusion_common::Result<Vec<crate::Expr>>
```



## datafusion_expr::utils::expand_qualified_wildcard

*Function*

Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.

```rust
fn expand_qualified_wildcard(qualifier: &datafusion_common::TableReference, schema: &datafusion_common::DFSchema, wildcard_options: Option<&crate::expr::WildcardOptions>) -> datafusion_common::Result<Vec<crate::Expr>>
```



## datafusion_expr::utils::expand_wildcard

*Function*

Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.

```rust
fn expand_wildcard(schema: &datafusion_common::DFSchema, plan: &crate::LogicalPlan, wildcard_options: Option<&crate::expr::WildcardOptions>) -> datafusion_common::Result<Vec<crate::Expr>>
```



## datafusion_expr::utils::expr_as_column_expr

*Function*

Convert any `Expr` to an `Expr::Column`.

```rust
fn expr_as_column_expr(expr: &crate::Expr, plan: &crate::LogicalPlan) -> datafusion_common::Result<crate::Expr>
```



## datafusion_expr::utils::expr_to_columns

*Function*

Recursively walk an expression tree, collecting the unique set of columns
referenced in the expression

```rust
fn expr_to_columns(expr: &crate::Expr, accum: & mut std::collections::HashSet<datafusion_common::Column>) -> datafusion_common::Result<()>
```



## datafusion_expr::utils::exprlist_to_fields

*Function*

Create schema fields from an expression list, for use in result set schema construction

This function converts a list of expressions into a list of complete schema fields,
making comprehensive determinations about each field's properties including:
- **Data type**: Resolved based on expression type and input schema context
- **Nullability**: Determined by expression-specific nullability rules
- **Metadata**: Computed based on expression type (preserving, merging, or generating new metadata)
- **Table reference scoping**: Establishing proper qualified field references

Each expression is converted to a field by calling [`Expr::to_field`], which performs
the complete field resolution process for all field properties.

# Returns

A `Result` containing a vector of `(Option<TableReference>, Arc<Field>)` tuples,
where each Field contains complete schema information (type, nullability, metadata)
and proper table reference scoping for the corresponding expression.

```rust
fn exprlist_to_fields<'a, impl IntoIterator<Item = &'a Expr>>(exprs: impl Trait, plan: &crate::LogicalPlan) -> datafusion_common::Result<Vec<(Option<datafusion_common::TableReference>, std::sync::Arc<arrow::datatypes::Field>)>>
```



## datafusion_expr::utils::find_aggregate_exprs

*Function*

Collect all deeply nested `Expr::AggregateFunction`.
They are returned in order of occurrence (depth
first), with duplicates omitted.

```rust
fn find_aggregate_exprs<'a, impl IntoIterator<Item = &'a Expr>>(exprs: impl Trait) -> Vec<crate::Expr>
```



## datafusion_expr::utils::find_column_exprs

*Function*

Collect all deeply nested `Expr::Column`'s. They are returned in order of
appearance (depth first), and may contain duplicates.

```rust
fn find_column_exprs(exprs: &[crate::Expr]) -> Vec<crate::Expr>
```



## datafusion_expr::utils::find_join_exprs

*Function*

Looks for correlating expressions: for example, a binary expression with one field from the subquery, and
one not in the subquery (closed upon from outer scope)

# Arguments

* `exprs` - List of expressions that may or may not be joins

# Return value

Tuple of (expressions containing joins, remaining non-join expressions)

```rust
fn find_join_exprs(exprs: Vec<&crate::Expr>) -> datafusion_common::Result<(Vec<crate::Expr>, Vec<crate::Expr>)>
```



## datafusion_expr::utils::find_out_reference_exprs

*Function*

Collect all deeply nested `Expr::OuterReferenceColumn`. They are returned in order of occurrence
(depth first), with duplicates omitted.

```rust
fn find_out_reference_exprs(expr: &crate::Expr) -> Vec<crate::Expr>
```



## datafusion_expr::utils::find_valid_equijoin_key_pair

*Function*

Give two sides of the equijoin predicate, return a valid join key pair.
If there is no valid join key pair, return None.

A valid join means:
1. All referenced column of the left side is from the left schema, and
   all referenced column of the right side is from the right schema.
2. Or opposite. All referenced column of the left side is from the right schema,
   and the right side is from the left schema.

```rust
fn find_valid_equijoin_key_pair(left_key: &crate::Expr, right_key: &crate::Expr, left_schema: &datafusion_common::DFSchema, right_schema: &datafusion_common::DFSchema) -> datafusion_common::Result<Option<(crate::Expr, crate::Expr)>>
```



## datafusion_expr::utils::find_window_exprs

*Function*

Collect all deeply nested `Expr::WindowFunction`. They are returned in order of occurrence
(depth first), with duplicates omitted.

```rust
fn find_window_exprs<'a, impl IntoIterator<Item = &'a Expr>>(exprs: impl Trait) -> Vec<crate::Expr>
```



## datafusion_expr::utils::format_state_name

*Function*

Build state name. State is the intermediate state of the aggregate function.

```rust
fn format_state_name(name: &str, state_name: &str) -> String
```



## datafusion_expr::utils::generate_signature_error_msg

*Function*

Creates a detailed error message for a function with wrong signature.

For example, a query like `select round(3.14, 1.1);` would yield:
```text
Error during planning: No function matches 'round(Float64, Float64)'. You might need to add explicit type casts.
    Candidate functions:
    round(Float64, Int64)
    round(Float32, Int64)
    round(Float64)
    round(Float32)
```

```rust
fn generate_signature_error_msg(func_name: &str, func_signature: datafusion_expr_common::signature::Signature, input_expr_types: &[arrow::datatypes::DataType]) -> String
```



## datafusion_expr::utils::generate_sort_key

*Function*

Generate a sort key for a given window expr's partition_by and order_by expr

```rust
fn generate_sort_key(partition_by: &[crate::Expr], order_by: &[crate::expr::Sort]) -> datafusion_common::Result<Vec<(crate::expr::Sort, bool)>>
```



## datafusion_expr::utils::group_window_expr_by_sort_keys

*Function*

Group a slice of window expression expr by their order by expressions

```rust
fn group_window_expr_by_sort_keys<impl IntoIterator<Item = Expr>>(window_expr: impl Trait) -> datafusion_common::Result<Vec<(Vec<(crate::expr::Sort, bool)>, Vec<crate::Expr>)>>
```



## datafusion_expr::utils::grouping_set_expr_count

*Function*

Count the number of distinct exprs in a list of group by expressions. If the
first element is a `GroupingSet` expression then it must be the only expr.

```rust
fn grouping_set_expr_count(group_expr: &[crate::Expr]) -> datafusion_common::Result<usize>
```



## datafusion_expr::utils::grouping_set_to_exprlist

*Function*

Find all distinct exprs in a list of group by expressions. If the
first element is a `GroupingSet` expression then it must be the only expr.

```rust
fn grouping_set_to_exprlist(group_expr: &[crate::Expr]) -> datafusion_common::Result<Vec<&crate::Expr>>
```



## datafusion_expr::utils::inspect_expr_pre

*Function*

Recursively inspect an [`Expr`] and all its children.

```rust
fn inspect_expr_pre<F, E>(expr: &crate::Expr, f: F) -> datafusion_common::Result<(), E>
```



## datafusion_expr::utils::iter_conjunction

*Function*

Iterate parts in a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`

See [`split_conjunction_owned`] for more details and an example.

```rust
fn iter_conjunction(expr: &crate::Expr) -> impl Trait
```



## datafusion_expr::utils::iter_conjunction_owned

*Function*

Iterate parts in a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`

See [`split_conjunction_owned`] for more details and an example.

```rust
fn iter_conjunction_owned(expr: crate::Expr) -> impl Trait
```



## datafusion_expr::utils::merge_schema

*Function*

merge inputs schema into a single schema.

This function merges schemas from multiple logical plan inputs using [`DFSchema::merge`].
Refer to that documentation for details on precedence and metadata handling.

```rust
fn merge_schema(inputs: &[&crate::LogicalPlan]) -> datafusion_common::DFSchema
```



## datafusion_expr::utils::only_or_err

*Function*

Returns the first (and only) element in a slice, or an error

# Arguments

* `slice` - The slice to extract from

# Return value

The first element, or an error

```rust
fn only_or_err<T>(slice: &[T]) -> datafusion_common::Result<&T>
```



## datafusion_expr::utils::powerset

*Function*

The [power set] (or powerset) of a set S is the set of all subsets of S, \
including the empty set and S itself.

Example:

If S is the set {x, y, z}, then all the subsets of S are \
 {} \
 {x} \
 {y} \
 {z} \
 {x, y} \
 {x, z} \
 {y, z} \
 {x, y, z} \
 and hence the power set of S is {{}, {x}, {y}, {z}, {x, y}, {x, z}, {y, z}, {x, y, z}}.

[power set]: https://en.wikipedia.org/wiki/Power_set

```rust
fn powerset<T>(slice: &[T]) -> datafusion_common::Result<Vec<Vec<&T>>>
```



## datafusion_expr::utils::split_binary

*Function*

Splits an binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`

See [`split_binary_owned`] for more details and an example.

```rust
fn split_binary(expr: &crate::Expr, op: crate::Operator) -> Vec<&crate::Expr>
```



## datafusion_expr::utils::split_binary_owned

*Function*

Splits an owned binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`

This is often used to "split" expressions such as `col1 = 5
AND col2 = 10` into [`col1 = 5`, `col2 = 10`];

# Example
```
# use datafusion_expr::{col, lit, Operator};
# use datafusion_expr::utils::split_binary_owned;
# use std::ops::Add;
// a=1 + b=2
let expr = col("a").eq(lit(1)).add(col("b").eq(lit(2)));

// [a=1, b=2]
let split = vec![col("a").eq(lit(1)), col("b").eq(lit(2))];

// use split_binary_owned to split them
assert_eq!(split_binary_owned(expr, Operator::Plus), split);
```

```rust
fn split_binary_owned(expr: crate::Expr, op: crate::Operator) -> Vec<crate::Expr>
```



## datafusion_expr::utils::split_conjunction

*Function*

Splits a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`

See [`split_conjunction_owned`] for more details and an example.

```rust
fn split_conjunction(expr: &crate::Expr) -> Vec<&crate::Expr>
```



## datafusion_expr::utils::split_conjunction_owned

*Function*

Splits an owned conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`

This is often used to "split" filter expressions such as `col1 = 5
AND col2 = 10` into [`col1 = 5`, `col2 = 10`];

# Example
```
# use datafusion_expr::{col, lit};
# use datafusion_expr::utils::split_conjunction_owned;
// a=1 AND b=2
let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));

// [a=1, b=2]
let split = vec![col("a").eq(lit(1)), col("b").eq(lit(2))];

// use split_conjunction_owned to split them
assert_eq!(split_conjunction_owned(expr), split);
```

```rust
fn split_conjunction_owned(expr: crate::Expr) -> Vec<crate::Expr>
```



