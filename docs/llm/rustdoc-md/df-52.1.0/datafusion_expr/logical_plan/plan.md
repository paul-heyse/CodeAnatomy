**datafusion_expr > logical_plan > plan**

# Module: logical_plan::plan

## Contents

**Structs**

- [`Aggregate`](#aggregate) - Aggregates its input based on a set of grouping and aggregate
- [`Analyze`](#analyze) - Runs the actual plan, and then prints the physical plan with
- [`ColumnUnnestList`](#columnunnestlist) - Represent the unnesting operation on a list column, such as the recursion depth and
- [`DescribeTable`](#describetable) - Describe the schema of table
- [`DistinctOn`](#distincton) - Removes duplicate rows from the input
- [`EmptyRelation`](#emptyrelation) - Relationship produces 0 or 1 placeholder rows with specified output schema
- [`Explain`](#explain) - Produces a relation with string representations of
- [`ExplainOption`](#explainoption) - Options for EXPLAIN
- [`Extension`](#extension) - Extension operator defined outside of DataFusion
- [`Filter`](#filter) - Filters rows from its input that do not match an
- [`Join`](#join) - Join two logical plans on one or more join columns
- [`Limit`](#limit) - Produces the first `n` tuples from its input and discards the rest.
- [`Projection`](#projection) - Evaluates an arbitrary list of expressions (essentially a
- [`RecursiveQuery`](#recursivequery) - A variadic query operation, Recursive CTE.
- [`Repartition`](#repartition)
- [`Sort`](#sort) - Sorts its input according to a list of sort expressions.
- [`Subquery`](#subquery) - Subquery
- [`SubqueryAlias`](#subqueryalias) - Aliased subquery
- [`TableScan`](#tablescan) - Produces rows from a table provider by reference or from the context
- [`Union`](#union) - Union multiple inputs
- [`Unnest`](#unnest) - Unnest a column that contains a nested list type. See
- [`Values`](#values) - Values expression. See
- [`Window`](#window) - Window its input based on a set of window spec and window function (e.g. SUM or RANK)

**Enums**

- [`Distinct`](#distinct) - Removes duplicate rows from the input
- [`FetchType`](#fetchtype) - Different types of fetch expression in Limit plan.
- [`LogicalPlan`](#logicalplan) - A `LogicalPlan` is a node in a tree of relational operators (such as
- [`Partitioning`](#partitioning) - Logical partitioning schemes supported by [`LogicalPlan::Repartition`]
- [`SkipType`](#skiptype) - Different types of skip expression in Limit plan.

**Functions**

- [`projection_schema`](#projection_schema) - Computes the schema of the result produced by applying a projection to the input logical plan.

---

## datafusion_expr::logical_plan::plan::Aggregate

*Struct*

Aggregates its input based on a set of grouping and aggregate
expressions (e.g. SUM).

# Output Schema

The output schema is the group expressions followed by the aggregate
expressions in order.

For example, given the input schema `"A", "B", "C"` and the aggregate
`SUM(A) GROUP BY C+B`, the output schema will be `"C+B", "SUM(A)"` where
"C+B" and "SUM(A)" are the names of the output columns. Note that "C+B" is a
single new column

**Fields:**
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `group_expr: Vec<crate::Expr>` - Grouping expressions
- `aggr_expr: Vec<crate::Expr>` - Aggregate expressions
- `schema: datafusion_common::DFSchemaRef` - The schema description of the aggregate output

**Methods:**

- `fn try_new(input: Arc<LogicalPlan>, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self>` - Create a new aggregate operator.
- `fn try_new_with_schema(input: Arc<LogicalPlan>, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>, schema: DFSchemaRef) -> Result<Self>` - Create a new aggregate operator using the provided schema to avoid the overhead of
- `fn group_expr_len(self: &Self) -> Result<usize>` - Get the length of the group by expression in the output schema
- `fn grouping_id_type(group_exprs: usize) -> DataType` - Returns the data type of the grouping id.

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Aggregate`
- **PartialEq**
  - `fn eq(self: &Self, other: &Aggregate) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::plan::Analyze

*Struct*

Runs the actual plan, and then prints the physical plan with
with execution metrics.

**Fields:**
- `verbose: bool` - Should extra detail be included?
- `input: std::sync::Arc<LogicalPlan>` - The logical plan that is being EXPLAIN ANALYZE'd
- `schema: datafusion_common::DFSchemaRef` - The output schema of the explain (2 columns of text)

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Analyze`
- **PartialEq**
  - `fn eq(self: &Self, other: &Analyze) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::ColumnUnnestList

*Struct*

Represent the unnesting operation on a list column, such as the recursion depth and
the output column name after unnesting

Example: given `ColumnUnnestList { output_column: "output_name", depth: 2 }`

```text
  input             output_name
 ┌─────────┐      ┌─────────┐
 │{{1,2}}  │      │ 1       │
 ├─────────┼─────►├─────────┤
 │{{3}}    │      │ 2       │
 ├─────────┤      ├─────────┤
 │{{4},{5}}│      │ 3       │
 └─────────┘      ├─────────┤
                  │ 4       │
                  ├─────────┤
                  │ 5       │
                  └─────────┘
```

**Fields:**
- `output_column: datafusion_common::Column`
- `depth: usize`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ColumnUnnestList) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ColumnUnnestList`
- **PartialEq**
  - `fn eq(self: &Self, other: &ColumnUnnestList) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::plan::DescribeTable

*Struct*

Describe the schema of table

# Example output:

```sql
> describe traces;
+--------------------+-----------------------------+-------------+
| column_name        | data_type                   | is_nullable |
+--------------------+-----------------------------+-------------+
| attributes         | Utf8                        | YES         |
| duration_nano      | Int64                       | YES         |
| end_time_unix_nano | Int64                       | YES         |
| service.name       | Dictionary(Int32, Utf8)     | YES         |
| span.kind          | Utf8                        | YES         |
| span.name          | Utf8                        | YES         |
| span_id            | Dictionary(Int32, Utf8)     | YES         |
| time               | Timestamp(Nanosecond, None) | NO          |
| trace_id           | Dictionary(Int32, Utf8)     | YES         |
| otel.status_code   | Utf8                        | YES         |
| parent_span_id     | Utf8                        | YES         |
+--------------------+-----------------------------+-------------+
```

**Fields:**
- `schema: std::sync::Arc<arrow::datatypes::Schema>` - Table schema
- `output_schema: datafusion_common::DFSchemaRef` - schema of describe table output

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &DescribeTable) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, _other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DescribeTable`



## datafusion_expr::logical_plan::plan::Distinct

*Enum*

Removes duplicate rows from the input

**Variants:**
- `All(std::sync::Arc<LogicalPlan>)` - Plain `DISTINCT` referencing all selection expressions
- `On(DistinctOn)` - The `Postgres` addition, allowing separate control over DISTINCT'd and selected columns

**Methods:**

- `fn input(self: &Self) -> &Arc<LogicalPlan>` - return a reference to the nodes input

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Distinct`
- **PartialEq**
  - `fn eq(self: &Self, other: &Distinct) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Distinct) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::logical_plan::plan::DistinctOn

*Struct*

Removes duplicate rows from the input

**Fields:**
- `on_expr: Vec<crate::Expr>` - The `DISTINCT ON` clause expression list
- `select_expr: Vec<crate::Expr>` - The selected projection expression list
- `sort_expr: Option<Vec<crate::expr::Sort>>` - The `ORDER BY` clause, whose initial expressions must match those of the `ON` clause when
- `input: std::sync::Arc<LogicalPlan>` - The logical plan that is being DISTINCT'd
- `schema: datafusion_common::DFSchemaRef` - The schema description of the DISTINCT ON output

**Methods:**

- `fn try_new(on_expr: Vec<Expr>, select_expr: Vec<Expr>, sort_expr: Option<Vec<SortExpr>>, input: Arc<LogicalPlan>) -> Result<Self>` - Create a new `DistinctOn` struct.
- `fn with_sort_expr(self: Self, sort_expr: Vec<SortExpr>) -> Result<Self>` - Try to update `self` with a new sort expressions.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DistinctOn`
- **PartialEq**
  - `fn eq(self: &Self, other: &DistinctOn) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::EmptyRelation

*Struct*

Relationship produces 0 or 1 placeholder rows with specified output schema
In most cases the output schema for `EmptyRelation` would be empty,
however, it can be non-empty typically for optimizer rules

**Fields:**
- `produce_one_row: bool` - Whether to produce a placeholder row
- `schema: datafusion_common::DFSchemaRef` - The schema description of the output

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> EmptyRelation`
- **PartialEq**
  - `fn eq(self: &Self, other: &EmptyRelation) -> bool`



## datafusion_expr::logical_plan::plan::Explain

*Struct*

Produces a relation with string representations of
various parts of the plan

See [the documentation] for more information

[the documentation]: https://datafusion.apache.org/user-guide/sql/explain.html

**Fields:**
- `verbose: bool` - Should extra (detailed, intermediate plans) be included?
- `explain_format: datafusion_common::format::ExplainFormat` - Output format for explain, if specified.
- `plan: std::sync::Arc<LogicalPlan>` - The logical plan that is being EXPLAIN'd
- `stringified_plans: Vec<StringifiedPlan>` - Represent the various stages plans have gone through
- `schema: datafusion_common::DFSchemaRef` - The output schema of the explain (2 columns of text)
- `logical_optimization_succeeded: bool` - Used by physical planner to check if should proceed with planning

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Explain`
- **PartialEq**
  - `fn eq(self: &Self, other: &Explain) -> bool`



## datafusion_expr::logical_plan::plan::ExplainOption

*Struct*

Options for EXPLAIN

**Fields:**
- `verbose: bool` - Include detailed debug info
- `analyze: bool` - Actually execute the plan and report metrics
- `format: datafusion_common::format::ExplainFormat` - Output syntax/format

**Methods:**

- `fn with_verbose(self: Self, verbose: bool) -> Self` - Builder‐style setter for `verbose`
- `fn with_analyze(self: Self, analyze: bool) -> Self` - Builder‐style setter for `analyze`
- `fn with_format(self: Self, format: ExplainFormat) -> Self` - Builder‐style setter for `format`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ExplainOption`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExplainOption) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



## datafusion_expr::logical_plan::plan::Extension

*Struct*

Extension operator defined outside of DataFusion

**Fields:**
- `node: std::sync::Arc<dyn UserDefinedLogicalNode>` - The runtime extension operator

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> Extension`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::FetchType

*Enum*

Different types of fetch expression in Limit plan.

**Variants:**
- `Literal(Option<usize>)` - The fetch expression is a literal value.
- `UnsupportedExpr` - Currently only supports expressions that can be folded into constants.



## datafusion_expr::logical_plan::plan::Filter

*Struct*

Filters rows from its input that do not match an
expression (essentially a WHERE clause with a predicate
expression).

Semantically, `<predicate>` is evaluated for each row of the input;
If the value of `<predicate>` is true, the input row is passed to
the output. If the value of `<predicate>` is false, the row is
discarded.

Filter should not be created directly but instead use `try_new()`
and that these fields are only pub to support pattern matching

**Fields:**
- `predicate: crate::Expr` - The predicate expression, which must have Boolean type.
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan

**Methods:**

- `fn try_new(predicate: Expr, input: Arc<LogicalPlan>) -> Result<Self>` - Create a new filter operator.
- `fn try_new_with_having(predicate: Expr, input: Arc<LogicalPlan>) -> Result<Self>` - Create a new filter operator for a having clause.

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Filter`
- **PartialEq**
  - `fn eq(self: &Self, other: &Filter) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Filter) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::logical_plan::plan::Join

*Struct*

Join two logical plans on one or more join columns

**Fields:**
- `left: std::sync::Arc<LogicalPlan>` - Left input
- `right: std::sync::Arc<LogicalPlan>` - Right input
- `on: Vec<(crate::Expr, crate::Expr)>` - Equijoin clause expressed as pairs of (left, right) join expressions
- `filter: Option<crate::Expr>` - Filters applied during join (non-equi conditions)
- `join_type: JoinType` - Join type
- `join_constraint: JoinConstraint` - Join constraint
- `schema: datafusion_common::DFSchemaRef` - The output schema, containing fields from the left and right inputs
- `null_equality: datafusion_common::NullEquality` - Defines the null equality for the join.

**Methods:**

- `fn try_new(left: Arc<LogicalPlan>, right: Arc<LogicalPlan>, on: Vec<(Expr, Expr)>, filter: Option<Expr>, join_type: JoinType, join_constraint: JoinConstraint, null_equality: NullEquality) -> Result<Self>` - Creates a new Join operator with automatically computed schema.
- `fn try_new_with_project_input(original: &LogicalPlan, left: Arc<LogicalPlan>, right: Arc<LogicalPlan>, column_on: (Vec<Column>, Vec<Column>)) -> Result<(Self, bool)>` - Create Join with input which wrapped with projection, this method is used in physical planning only to help

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Join`
- **PartialEq**
  - `fn eq(self: &Self, other: &Join) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::plan::Limit

*Struct*

Produces the first `n` tuples from its input and discards the rest.

**Fields:**
- `skip: Option<Box<crate::Expr>>` - Number of rows to skip before fetch
- `fetch: Option<Box<crate::Expr>>` - Maximum number of rows to fetch,
- `input: std::sync::Arc<LogicalPlan>` - The logical plan

**Methods:**

- `fn get_skip_type(self: &Self) -> Result<SkipType>` - Get the skip type from the limit plan.
- `fn get_fetch_type(self: &Self) -> Result<FetchType>` - Get the fetch type from the limit plan.

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Limit`
- **PartialEq**
  - `fn eq(self: &Self, other: &Limit) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Limit) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::plan::LogicalPlan

*Enum*

A `LogicalPlan` is a node in a tree of relational operators (such as
Projection or Filter).

Represents transforming an input relation (table) to an output relation
(table) with a potentially different schema. Plans form a dataflow tree
where data flows from leaves up to the root to produce the query result.

`LogicalPlan`s can be created by the SQL query planner, the DataFrame API,
or programmatically (for example custom query languages).

# See also:
* [`Expr`]: For the expressions that are evaluated by the plan
* [`LogicalPlanBuilder`]: For building `LogicalPlan`s
* [`tree_node`]: To inspect and rewrite `LogicalPlan`s

[`tree_node`]: crate::logical_plan::tree_node

# Examples

## Creating a LogicalPlan from SQL:

See [`SessionContext::sql`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql)

## Creating a LogicalPlan from the DataFrame API:

See [`DataFrame::logical_plan`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.logical_plan)

## Creating a LogicalPlan programmatically:

See [`LogicalPlanBuilder`]

# Visiting and Rewriting `LogicalPlan`s

Using the [`tree_node`] API, you can recursively walk all nodes in a
`LogicalPlan`. For example, to find all column references in a plan:

```
# use std::collections::HashSet;
# use arrow::datatypes::{DataType, Field, Schema};
# use datafusion_expr::{Expr, col, lit, LogicalPlan, LogicalPlanBuilder, table_scan};
# use datafusion_common::tree_node::{TreeNodeRecursion, TreeNode};
# use datafusion_common::{Column, Result};
# fn employee_schema() -> Schema {
#    Schema::new(vec![
#           Field::new("name", DataType::Utf8, false),
#           Field::new("salary", DataType::Int32, false),
#       ])
#   }
// Projection(name, salary)
//   Filter(salary > 1000)
//     TableScan(employee)
# fn main() -> Result<()> {
let plan = table_scan(Some("employee"), &employee_schema(), None)?
 .filter(col("salary").gt(lit(1000)))?
 .project(vec![col("name")])?
 .build()?;

// use apply to walk the plan and collect all expressions
let mut expressions = HashSet::new();
plan.apply(|node| {
  // collect all expressions in the plan
  node.apply_expressions(|expr| {
   expressions.insert(expr.clone());
   Ok(TreeNodeRecursion::Continue) // control walk of expressions
  })?;
  Ok(TreeNodeRecursion::Continue) // control walk of plan nodes
}).unwrap();

// we found the expression in projection and filter
assert_eq!(expressions.len(), 2);
println!("Found expressions: {:?}", expressions);
// found predicate in the Filter: employee.salary > 1000
let salary = Expr::Column(Column::new(Some("employee"), "salary"));
assert!(expressions.contains(&salary.gt(lit(1000))));
// found projection in the Projection: employee.name
let name = Expr::Column(Column::new(Some("employee"), "name"));
assert!(expressions.contains(&name));
# Ok(())
# }
```

You can also rewrite plans using the [`tree_node`] API. For example, to
replace the filter predicate in a plan:

```
# use std::collections::HashSet;
# use arrow::datatypes::{DataType, Field, Schema};
# use datafusion_expr::{Expr, col, lit, LogicalPlan, LogicalPlanBuilder, table_scan};
# use datafusion_common::tree_node::{TreeNodeRecursion, TreeNode};
# use datafusion_common::{Column, Result};
# fn employee_schema() -> Schema {
#    Schema::new(vec![
#           Field::new("name", DataType::Utf8, false),
#           Field::new("salary", DataType::Int32, false),
#       ])
#   }
// Projection(name, salary)
//   Filter(salary > 1000)
//     TableScan(employee)
# fn main() -> Result<()> {
use datafusion_common::tree_node::Transformed;
let plan = table_scan(Some("employee"), &employee_schema(), None)?
 .filter(col("salary").gt(lit(1000)))?
 .project(vec![col("name")])?
 .build()?;

// use transform to rewrite the plan
let transformed_result = plan.transform(|node| {
  // when we see the filter node
  if let LogicalPlan::Filter(mut filter) = node {
    // replace predicate with salary < 2000
    filter.predicate = Expr::Column(Column::new(Some("employee"), "salary")).lt(lit(2000));
    let new_plan = LogicalPlan::Filter(filter);
    return Ok(Transformed::yes(new_plan)); // communicate the node was changed
  }
  // return the node unchanged
  Ok(Transformed::no(node))
}).unwrap();

// Transformed result contains rewritten plan and information about
// whether the plan was changed
assert!(transformed_result.transformed);
let rewritten_plan = transformed_result.data;

// we found the filter
assert_eq!(rewritten_plan.display_indent().to_string(),
"Projection: employee.name\
\n  Filter: employee.salary < Int32(2000)\
\n    TableScan: employee");
# Ok(())
# }
```

**Variants:**
- `Projection(Projection)` - Evaluates an arbitrary list of expressions (essentially a
- `Filter(Filter)` - Filters rows from its input that do not match an
- `Window(Window)` - Windows input based on a set of window spec and window
- `Aggregate(Aggregate)` - Aggregates its input based on a set of grouping and aggregate
- `Sort(Sort)` - Sorts its input according to a list of sort expressions. This
- `Join(Join)` - Join two logical plans on one or more join columns.
- `Repartition(Repartition)` - Repartitions the input based on a partitioning scheme. This is
- `Union(Union)` - Union multiple inputs with the same schema into a single
- `TableScan(TableScan)` - Produces rows from a [`TableSource`], used to implement SQL
- `EmptyRelation(EmptyRelation)` - Produces no rows: An empty relation with an empty schema that
- `Subquery(Subquery)` - Produces the output of running another query.  This is used to
- `SubqueryAlias(SubqueryAlias)` - Aliased relation provides, or changes, the name of a relation.
- `Limit(Limit)` - Skip some number of rows, and then fetch some number of rows.
- `Statement(crate::logical_plan::Statement)` - A DataFusion [`Statement`] such as `SET VARIABLE` or `START TRANSACTION`
- `Values(Values)` - Values expression. See
- `Explain(Explain)` - Produces a relation with string representations of
- `Analyze(Analyze)` - Runs the input, and prints annotated physical plan as a string
- `Extension(Extension)` - Extension operator defined outside of DataFusion. This is used
- `Distinct(Distinct)` - Remove duplicate rows from the input. This is used to
- `Dml(crate::logical_plan::DmlStatement)` - Data Manipulation Language (DML): Insert / Update / Delete
- `Ddl(super::DdlStatement)` - Data Definition Language (DDL): CREATE / DROP TABLES / VIEWS / SCHEMAS
- `Copy(super::dml::CopyTo)` - `COPY TO` for writing plan results to files
- `DescribeTable(DescribeTable)` - Describe the schema of the table. This is used to implement the
- `Unnest(Unnest)` - Unnest a column that contains a nested list type such as an
- `RecursiveQuery(RecursiveQuery)` - A variadic query (e.g. "Recursive CTEs")

**Methods:**

- `fn schema(self: &Self) -> &DFSchemaRef` - Get a reference to the logical plan's schema
- `fn fallback_normalize_schemas(self: &Self) -> Vec<&DFSchema>` - Used for normalizing columns, as the fallback schemas to the main schema
- `fn explain_schema() -> SchemaRef` - Returns the (fixed) output schema for explain plans
- `fn describe_schema() -> Schema` - Returns the (fixed) output schema for `DESCRIBE` plans
- `fn expressions(self: &LogicalPlan) -> Vec<Expr>` - Returns all expressions (non-recursively) evaluated by the current
- `fn all_out_ref_exprs(self: &LogicalPlan) -> Vec<Expr>` - Returns all the out reference(correlated) expressions (recursively) in the current
- `fn inputs(self: &Self) -> Vec<&LogicalPlan>` - Returns all inputs / children of this `LogicalPlan` node.
- `fn using_columns(self: &Self) -> Result<Vec<HashSet<Column>>, DataFusionError>` - returns all `Using` join columns in a logical plan
- `fn head_output_expr(self: &Self) -> Result<Option<Expr>>` - returns the first output expression of this `LogicalPlan` node.
- `fn recompute_schema(self: Self) -> Result<Self>` - Recomputes schema and type information for this LogicalPlan if needed.
- `fn with_new_exprs(self: &Self, expr: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<LogicalPlan>` - Returns a new `LogicalPlan` based on `self` with inputs and
- `fn check_invariants(self: &Self, check: InvariantLevel) -> Result<()>` - checks that the plan conforms to the listed invariant level, returning an Error if not
- `fn with_param_values<impl Into<ParamValues>>(self: Self, param_values: impl Trait) -> Result<LogicalPlan>` - Replaces placeholder param values (like `$1`, `$2`) in [`LogicalPlan`]
- `fn max_rows(self: &LogicalPlan) -> Option<usize>` - Returns the maximum number of rows that this plan can output, if known.
- `fn contains_outer_reference(self: &Self) -> bool` - If this node's expressions contains any references to an outer subquery
- `fn columnized_output_exprs(self: &Self) -> Result<Vec<(&Expr, Column)>>` - Get the output expressions and their corresponding columns.
- `fn apply_expressions<F>(self: &Self, f: F) -> Result<TreeNodeRecursion>` - Calls `f` on all expressions in the current `LogicalPlan` node.
- `fn map_expressions<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Rewrites all expressions in the current `LogicalPlan` node using `f`.
- `fn visit_with_subqueries<V>(self: &Self, visitor: & mut V) -> Result<TreeNodeRecursion>` - Visits a plan similarly to [`Self::visit`], including subqueries that
- `fn rewrite_with_subqueries<R>(self: Self, rewriter: & mut R) -> Result<Transformed<Self>>` - Similarly to [`Self::rewrite`], rewrites this node and its inputs using `f`,
- `fn apply_with_subqueries<F>(self: &Self, f: F) -> Result<TreeNodeRecursion>` - Similarly to [`Self::apply`], calls `f` on this node and all its inputs,
- `fn transform_with_subqueries<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Similarly to [`Self::transform`], rewrites this node and its inputs using `f`,
- `fn transform_down_with_subqueries<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Similarly to [`Self::transform_down`], rewrites this node and its inputs using `f`,
- `fn transform_up_with_subqueries<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Similarly to [`Self::transform_up`], rewrites this node and its inputs using `f`,
- `fn transform_down_up_with_subqueries<FD, FU>(self: Self, f_down: FD, f_up: FU) -> Result<Transformed<Self>>` - Similarly to [`Self::transform_down`], rewrites this node and its inputs using `f`,
- `fn apply_subqueries<F>(self: &Self, f: F) -> Result<TreeNodeRecursion>` - Similarly to [`Self::apply`], calls `f` on  this node and its inputs
- `fn map_subqueries<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Similarly to [`Self::map_children`], rewrites all subqueries that may
- `fn replace_params_with_values(self: Self, param_values: &ParamValues) -> Result<LogicalPlan>` - Return a `LogicalPlan` with all placeholders (e.g $1 $2,
- `fn get_parameter_names(self: &Self) -> Result<HashSet<String>>` - Walk the logical plan, find any `Placeholder` tokens, and return a set of their names.
- `fn get_parameter_types(self: &Self) -> Result<HashMap<String, Option<DataType>>, DataFusionError>` - Walk the logical plan, find any `Placeholder` tokens, and return a map of their IDs and DataTypes
- `fn get_parameter_fields(self: &Self) -> Result<HashMap<String, Option<FieldRef>>, DataFusionError>` - Walk the logical plan, find any `Placeholder` tokens, and return a map of their IDs and FieldRefs
- `fn display_indent(self: &Self) -> impl Trait` - Return a `format`able structure that produces a single line
- `fn display_indent_schema(self: &Self) -> impl Trait` - Return a `format`able structure that produces a single line
- `fn display_pg_json(self: &Self) -> impl Trait` - Return a displayable structure that produces plan in postgresql JSON format.
- `fn display_graphviz(self: &Self) -> impl Trait` - Return a `format`able structure that produces lines meant for
- `fn display(self: &Self) -> impl Trait` - Return a `format`able structure with the a human readable

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> LogicalPlan`
- **TreeNode**
  - `fn apply_children<'n, F>(self: &'n Self, f: F) -> Result<TreeNodeRecursion>`
  - `fn map_children<F>(self: Self, f: F) -> Result<Transformed<Self>>` - Applies `f` to each child (input) of this plan node, rewriting them *in place.*
- **PartialEq**
  - `fn eq(self: &Self, other: &LogicalPlan) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &LogicalPlan) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ToStringifiedPlan**
  - `fn to_stringified(self: &Self, plan_type: PlanType) -> StringifiedPlan`
- **Default**
  - `fn default() -> Self`
- **TreeNodeContainer**
  - `fn apply_elements<F>(self: &'a Self, f: F) -> Result<TreeNodeRecursion>`
  - `fn map_elements<F>(self: Self, f: F) -> Result<Transformed<Self>>`



## datafusion_expr::logical_plan::plan::Partitioning

*Enum*

Logical partitioning schemes supported by [`LogicalPlan::Repartition`]

See [`Partitioning`] for more details on partitioning

[`Partitioning`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/enum.Partitioning.html#

**Variants:**
- `RoundRobinBatch(usize)` - Allocate batches using a round-robin algorithm and the specified number of partitions
- `Hash(Vec<crate::Expr>, usize)` - Allocate rows based on a hash of one of more expressions and the specified number
- `DistributeBy(Vec<crate::Expr>)` - The DISTRIBUTE BY clause is used to repartition the data based on the input expressions

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Partitioning`
- **PartialEq**
  - `fn eq(self: &Self, other: &Partitioning) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Partitioning) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::Projection

*Struct*

Evaluates an arbitrary list of expressions (essentially a
SELECT with an expression list) on its input.

**Fields:**
- `expr: Vec<crate::Expr>` - The list of expressions
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `schema: datafusion_common::DFSchemaRef` - The schema description of the output

**Methods:**

- `fn try_new(expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self>` - Create a new Projection
- `fn try_new_with_schema(expr: Vec<Expr>, input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Result<Self>` - Create a new Projection using the specified output schema
- `fn new_from_schema(input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Self` - Create a new Projection using the specified output schema

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> Projection`
- **PartialEq**
  - `fn eq(self: &Self, other: &Projection) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::RecursiveQuery

*Struct*

A variadic query operation, Recursive CTE.

# Recursive Query Evaluation

From the [Postgres Docs]:

1. Evaluate the non-recursive term. For `UNION` (but not `UNION ALL`),
   discard duplicate rows. Include all remaining rows in the result of the
   recursive query, and also place them in a temporary working table.

2. So long as the working table is not empty, repeat these steps:

* Evaluate the recursive term, substituting the current contents of the
  working table for the recursive self-reference. For `UNION` (but not `UNION
  ALL`), discard duplicate rows and rows that duplicate any previous result
  row. Include all remaining rows in the result of the recursive query, and
  also place them in a temporary intermediate table.

* Replace the contents of the working table with the contents of the
  intermediate table, then empty the intermediate table.

[Postgres Docs]: https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-RECURSIVE

**Fields:**
- `name: String` - Name of the query
- `static_term: std::sync::Arc<LogicalPlan>` - The static term (initial contents of the working table)
- `recursive_term: std::sync::Arc<LogicalPlan>` - The recursive term (evaluated on the contents of the working table until
- `is_distinct: bool` - Should the output of the recursive term be deduplicated (`UNION`) or

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RecursiveQuery`
- **PartialEq**
  - `fn eq(self: &Self, other: &RecursiveQuery) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &RecursiveQuery) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::Repartition

*Struct*

**Fields:**
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `partitioning_scheme: Partitioning` - The partitioning scheme

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Repartition`
- **PartialEq**
  - `fn eq(self: &Self, other: &Repartition) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Repartition) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::SkipType

*Enum*

Different types of skip expression in Limit plan.

**Variants:**
- `Literal(usize)` - The skip expression is a literal value.
- `UnsupportedExpr` - Currently only supports expressions that can be folded into constants.



## datafusion_expr::logical_plan::plan::Sort

*Struct*

Sorts its input according to a list of sort expressions.

**Fields:**
- `expr: Vec<crate::expr::Sort>` - The sort expressions
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `fetch: Option<usize>` - Optional fetch limit

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Sort) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Sort) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Sort`



## datafusion_expr::logical_plan::plan::Subquery

*Struct*

Subquery

**Fields:**
- `subquery: std::sync::Arc<LogicalPlan>` - The subquery
- `outer_ref_columns: Vec<crate::Expr>` - The outer references used in the subquery
- `spans: datafusion_common::Spans` - Span information for subquery projection columns

**Methods:**

- `fn try_from_expr(plan: &Expr) -> Result<&Subquery>`
- `fn with_plan(self: &Self, plan: Arc<LogicalPlan>) -> Subquery`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Subquery) -> $crate::option::Option<$crate::cmp::Ordering>`
- **NormalizeEq**
  - `fn normalize_eq(self: &Self, other: &Self) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Normalizeable**
  - `fn can_normalize(self: &Self) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Subquery`
- **PartialEq**
  - `fn eq(self: &Self, other: &Subquery) -> bool`



## datafusion_expr::logical_plan::plan::SubqueryAlias

*Struct*

Aliased subquery

**Fields:**
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `alias: datafusion_common::TableReference` - The alias for the input relation
- `schema: datafusion_common::DFSchemaRef` - The schema with qualified field names

**Methods:**

- `fn try_new<impl Into<TableReference>>(plan: Arc<LogicalPlan>, alias: impl Trait) -> Result<Self>`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SubqueryAlias`
- **PartialEq**
  - `fn eq(self: &Self, other: &SubqueryAlias) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::TableScan

*Struct*

Produces rows from a table provider by reference or from the context

**Fields:**
- `table_name: datafusion_common::TableReference` - The name of the table
- `source: std::sync::Arc<dyn TableSource>` - The source of the table
- `projection: Option<Vec<usize>>` - Optional column indices to use as a projection
- `projected_schema: datafusion_common::DFSchemaRef` - The schema description of the output
- `filters: Vec<crate::Expr>` - Optional expressions to be used as filters by the table provider
- `fetch: Option<usize>` - Optional number of rows to read

**Methods:**

- `fn try_new<impl Into<TableReference>>(table_name: impl Trait, table_source: Arc<dyn TableSource>, projection: Option<Vec<usize>>, filters: Vec<Expr>, fetch: Option<usize>) -> Result<Self>` - Initialize TableScan with appropriate schema from the given

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> TableScan`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`



## datafusion_expr::logical_plan::plan::Union

*Struct*

Union multiple inputs

**Fields:**
- `inputs: Vec<std::sync::Arc<LogicalPlan>>` - Inputs to merge
- `schema: datafusion_common::DFSchemaRef` - Union schema. Should be the same for all inputs.

**Methods:**

- `fn try_new(inputs: Vec<Arc<LogicalPlan>>) -> Result<Self>` - Constructs new Union instance deriving schema from inputs.
- `fn try_new_with_loose_types(inputs: Vec<Arc<LogicalPlan>>) -> Result<Self>` - Constructs new Union instance deriving schema from inputs.
- `fn try_new_by_name(inputs: Vec<Arc<LogicalPlan>>) -> Result<Self>` - Constructs a new Union instance that combines rows from different tables by name,

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Union`
- **PartialEq**
  - `fn eq(self: &Self, other: &Union) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::plan::Unnest

*Struct*

Unnest a column that contains a nested list type. See
[`UnnestOptions`] for more details.

**Fields:**
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `exec_columns: Vec<datafusion_common::Column>` - Columns to run unnest on, can be a list of (List/Struct) columns
- `list_type_columns: Vec<(usize, ColumnUnnestList)>` - refer to the indices(in the input schema) of columns
- `struct_type_columns: Vec<usize>` - refer to the indices (in the input schema) of columns
- `dependency_indices: Vec<usize>` - Having items aligned with the output columns
- `schema: datafusion_common::DFSchemaRef` - The output schema, containing the unnested field column.
- `options: datafusion_common::UnnestOptions` - Options

**Methods:**

- `fn try_new(input: Arc<LogicalPlan>, exec_columns: Vec<Column>, options: UnnestOptions) -> Result<Self>`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Unnest`
- **PartialEq**
  - `fn eq(self: &Self, other: &Unnest) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`



## datafusion_expr::logical_plan::plan::Values

*Struct*

Values expression. See
[Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
documentation for more details.

**Fields:**
- `schema: datafusion_common::DFSchemaRef` - The table schema
- `values: Vec<Vec<crate::Expr>>` - Values

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Values`
- **PartialEq**
  - `fn eq(self: &Self, other: &Values) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::plan::Window

*Struct*

Window its input based on a set of window spec and window function (e.g. SUM or RANK)

# Output Schema

The output schema is the input schema followed by the window function
expressions, in order.

For example, given the input schema `"A", "B", "C"` and the window function
`SUM(A) OVER (PARTITION BY B+1 ORDER BY C)`, the output schema will be `"A",
"B", "C", "SUM(A) OVER ..."` where `"SUM(A) OVER ..."` is the name of the
output column.

Note that the `PARTITION BY` expression "B+1" is not produced in the output
schema.

**Fields:**
- `input: std::sync::Arc<LogicalPlan>` - The incoming logical plan
- `window_expr: Vec<crate::Expr>` - The window function expression
- `schema: datafusion_common::DFSchemaRef` - The schema description of the window output

**Methods:**

- `fn try_new(window_expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self>` - Create a new window operator.
- `fn try_new_with_schema(window_expr: Vec<Expr>, input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Result<Self>` - Create a new window function using the provided schema to avoid the overhead of

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Window`
- **PartialEq**
  - `fn eq(self: &Self, other: &Window) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::plan::projection_schema

*Function*

Computes the schema of the result produced by applying a projection to the input logical plan.

# Arguments

* `input`: A reference to the input `LogicalPlan` for which the projection schema
  will be computed.
* `exprs`: A slice of `Expr` expressions representing the projection operation to apply.

# Metadata Handling

- **Schema-level metadata**: Passed through unchanged from the input schema
- **Field-level metadata**: Determined by each expression via [`exprlist_to_fields`], which
  calls [`Expr::to_field`] to handle expression-specific metadata (literals, aliases, etc.)

# Returns

A `Result` containing an `Arc<DFSchema>` representing the schema of the result
produced by the projection operation. If the schema computation is successful,
the `Result` will contain the schema; otherwise, it will contain an error.

```rust
fn projection_schema(input: &LogicalPlan, exprs: &[crate::Expr]) -> datafusion_common::Result<std::sync::Arc<datafusion_common::DFSchema>>
```



