**datafusion_expr > logical_plan > builder**

# Module: logical_plan::builder

## Contents

**Structs**

- [`LogicalPlanBuilder`](#logicalplanbuilder) - Builder for logical plans
- [`LogicalPlanBuilderOptions`](#logicalplanbuilderoptions) - Options for [`LogicalPlanBuilder`]
- [`LogicalTableSource`](#logicaltablesource) - Basic TableSource implementation intended for use in tests and documentation. It is expected

**Functions**

- [`add_group_by_exprs_from_dependencies`](#add_group_by_exprs_from_dependencies) - Add additional "synthetic" group by expressions based on functional
- [`build_join_schema`](#build_join_schema) - Creates a schema for a join operation.
- [`get_struct_unnested_columns`](#get_struct_unnested_columns)
- [`project`](#project) - Create Projection
- [`requalify_sides_if_needed`](#requalify_sides_if_needed) - (Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
- [`subquery_alias`](#subquery_alias) - Create a SubqueryAlias to wrap a LogicalPlan.
- [`table_scan`](#table_scan) - Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
- [`table_scan_with_filter_and_fetch`](#table_scan_with_filter_and_fetch) - Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
- [`table_scan_with_filters`](#table_scan_with_filters) - Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
- [`table_source`](#table_source)
- [`table_source_with_constraints`](#table_source_with_constraints)
- [`union`](#union) - Union two [`LogicalPlan`]s.
- [`union_by_name`](#union_by_name) - Like [`union`], but combine rows from different tables by name, rather than
- [`unique_field_aliases`](#unique_field_aliases) - Returns aliases to make field names unique.
- [`unnest`](#unnest) - Create a [`LogicalPlan::Unnest`] plan
- [`unnest_with_options`](#unnest_with_options) - Create a [`LogicalPlan::Unnest`] plan with options
- [`validate_unique_names`](#validate_unique_names) - Errors if one or more expressions have equal names.
- [`wrap_projection_for_join_if_necessary`](#wrap_projection_for_join_if_necessary) - Wrap projection for a plan, if the join keys contains normal expression.

**Constants**

- [`UNNAMED_TABLE`](#unnamed_table) - Default table name for unnamed table

---

## datafusion_expr::logical_plan::builder::LogicalPlanBuilder

*Struct*

Builder for logical plans

# Example building a simple plan
```
# use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
# use datafusion_common::Result;
# use arrow::datatypes::{Schema, DataType, Field};
#
# fn main() -> Result<()> {
#
# fn employee_schema() -> Schema {
#    Schema::new(vec![
#           Field::new("id", DataType::Int32, false),
#           Field::new("first_name", DataType::Utf8, false),
#           Field::new("last_name", DataType::Utf8, false),
#           Field::new("state", DataType::Utf8, false),
#           Field::new("salary", DataType::Int32, false),
#       ])
#   }
#
// Create a plan similar to
// SELECT last_name
// FROM employees
// WHERE salary < 1000
let plan = table_scan(Some("employee"), &employee_schema(), None)?
 // Keep only rows where salary < 1000
 .filter(col("salary").lt(lit(1000)))?
 // only show "last_name" in the final results
 .project(vec![col("last_name")])?
 .build()?;

// Convert from plan back to builder
let builder = LogicalPlanBuilder::from(plan);

# Ok(())
# }
```

**Methods:**

- `fn new(plan: LogicalPlan) -> Self` - Create a builder from an existing plan
- `fn new_from_arc(plan: Arc<LogicalPlan>) -> Self` - Create a builder from an existing plan
- `fn with_options(self: Self, options: LogicalPlanBuilderOptions) -> Self`
- `fn schema(self: &Self) -> &DFSchemaRef` - Return the output schema of the plan build so far
- `fn plan(self: &Self) -> &LogicalPlan` - Return the LogicalPlan of the plan build so far
- `fn empty(produce_one_row: bool) -> Self` - Create an empty relation.
- `fn to_recursive_query(self: Self, name: String, recursive_term: LogicalPlan, is_distinct: bool) -> Result<Self>` - Convert a regular plan into a recursive query.
- `fn values(values: Vec<Vec<Expr>>) -> Result<Self>` - Create a values list based relation, and the schema is inferred from data, consuming
- `fn values_with_schema(values: Vec<Vec<Expr>>, schema: &DFSchemaRef) -> Result<Self>` - Create a values list based relation, and the schema is inferred from data itself or table schema if provided, consuming
- `fn scan<impl Into<TableReference>>(table_name: impl Trait, table_source: Arc<dyn TableSource>, projection: Option<Vec<usize>>) -> Result<Self>` - Convert a table provider into a builder with a TableScan
- `fn copy_to(input: LogicalPlan, output_url: String, file_type: Arc<dyn FileType>, options: HashMap<String, String>, partition_by: Vec<String>) -> Result<Self>` - Create a [CopyTo] for copying the contents of this builder to the specified file(s)
- `fn insert_into<impl Into<TableReference>>(input: LogicalPlan, table_name: impl Trait, target: Arc<dyn TableSource>, insert_op: InsertOp) -> Result<Self>` - Create a [`DmlStatement`] for inserting the contents of this builder into the named table.
- `fn scan_with_filters<impl Into<TableReference>>(table_name: impl Trait, table_source: Arc<dyn TableSource>, projection: Option<Vec<usize>>, filters: Vec<Expr>) -> Result<Self>` - Convert a table provider into a builder with a TableScan
- `fn scan_with_filters_fetch<impl Into<TableReference>>(table_name: impl Trait, table_source: Arc<dyn TableSource>, projection: Option<Vec<usize>>, filters: Vec<Expr>, fetch: Option<usize>) -> Result<Self>` - Convert a table provider into a builder with a TableScan with filter and fetch
- `fn window_plan<impl IntoIterator<Item = Expr>>(input: LogicalPlan, window_exprs: impl Trait) -> Result<LogicalPlan>` - Wrap a plan in a window
- `fn project<impl Into<SelectExpr>, impl IntoIterator<Item = impl Into<SelectExpr>>>(self: Self, expr: impl Trait) -> Result<Self>` - Apply a projection without alias.
- `fn project_with_validation<impl Into<SelectExpr>>(self: Self, expr: Vec<(impl Trait, bool)>) -> Result<Self>` - Apply a projection without alias with optional validation
- `fn select<impl IntoIterator<Item = usize>>(self: Self, indices: impl Trait) -> Result<Self>` - Select the given column indices
- `fn filter<impl Into<Expr>>(self: Self, expr: impl Trait) -> Result<Self>` - Apply a filter
- `fn having<impl Into<Expr>>(self: Self, expr: impl Trait) -> Result<Self>` - Apply a filter which is used for a having clause
- `fn prepare(self: Self, name: String, fields: Vec<FieldRef>) -> Result<Self>` - Make a builder for a prepare logical plan from the builder's plan
- `fn limit(self: Self, skip: usize, fetch: Option<usize>) -> Result<Self>` - Limit the number of rows returned
- `fn limit_by_expr(self: Self, skip: Option<Expr>, fetch: Option<Expr>) -> Result<Self>` - Limit the number of rows returned
- `fn alias<impl Into<TableReference>>(self: Self, alias: impl Trait) -> Result<Self>` - Apply an alias
- `fn sort_by<impl Into<Expr>, impl IntoIterator<Item = impl Into<Expr>> + Clone>(self: Self, expr: impl Trait) -> Result<Self>` - Apply a sort by provided expressions with default direction
- `fn sort<impl Into<SortExpr>, impl IntoIterator<Item = impl Into<SortExpr>> + Clone>(self: Self, sorts: impl Trait) -> Result<Self>`
- `fn sort_with_limit<impl Into<SortExpr>, impl IntoIterator<Item = impl Into<SortExpr>> + Clone>(self: Self, sorts: impl Trait, fetch: Option<usize>) -> Result<Self>` - Apply a sort
- `fn union(self: Self, plan: LogicalPlan) -> Result<Self>` - Apply a union, preserving duplicate rows
- `fn union_by_name(self: Self, plan: LogicalPlan) -> Result<Self>` - Apply a union by name, preserving duplicate rows
- `fn union_by_name_distinct(self: Self, plan: LogicalPlan) -> Result<Self>` - Apply a union by name, removing duplicate rows
- `fn union_distinct(self: Self, plan: LogicalPlan) -> Result<Self>` - Apply a union, removing duplicate rows
- `fn distinct(self: Self) -> Result<Self>` - Apply deduplication: Only distinct (different) values are returned)
- `fn distinct_on(self: Self, on_expr: Vec<Expr>, select_expr: Vec<Expr>, sort_expr: Option<Vec<SortExpr>>) -> Result<Self>` - Project first values of the specified expression list according to the provided
- `fn join<impl Into<Column>, impl Into<Column>>(self: Self, right: LogicalPlan, join_type: JoinType, join_keys: (Vec<impl Trait>, Vec<impl Trait>), filter: Option<Expr>) -> Result<Self>` - Apply a join to `right` using explicitly specified columns and an
- `fn join_on<impl IntoIterator<Item = Expr>>(self: Self, right: LogicalPlan, join_type: JoinType, on_exprs: impl Trait) -> Result<Self>` - Apply a join using the specified expressions.
- `fn join_detailed<impl Into<Column>, impl Into<Column>>(self: Self, right: LogicalPlan, join_type: JoinType, join_keys: (Vec<impl Trait>, Vec<impl Trait>), filter: Option<Expr>, null_equality: NullEquality) -> Result<Self>` - Apply a join with on constraint and specified null equality.
- `fn join_using(self: Self, right: LogicalPlan, join_type: JoinType, using_keys: Vec<Column>) -> Result<Self>` - Apply a join with using constraint, which duplicates all join columns in output schema.
- `fn cross_join(self: Self, right: LogicalPlan) -> Result<Self>` - Apply a cross join
- `fn repartition(self: Self, partitioning_scheme: Partitioning) -> Result<Self>` - Repartition
- `fn window<impl Into<Expr>, impl IntoIterator<Item = impl Into<Expr>>>(self: Self, window_expr: impl Trait) -> Result<Self>` - Apply a window functions to extend the schema
- `fn aggregate<impl Into<Expr>, impl IntoIterator<Item = impl Into<Expr>>, impl Into<Expr>, impl IntoIterator<Item = impl Into<Expr>>>(self: Self, group_expr: impl Trait, aggr_expr: impl Trait) -> Result<Self>` - Apply an aggregate: grouping on the `group_expr` expressions
- `fn explain(self: Self, verbose: bool, analyze: bool) -> Result<Self>` - Create an expression to represent the explanation of the plan
- `fn explain_option_format(self: Self, explain_option: ExplainOption) -> Result<Self>` - Create an expression to represent the explanation of the plan
- `fn intersect(left_plan: LogicalPlan, right_plan: LogicalPlan, is_all: bool) -> Result<LogicalPlan>` - Process intersect set operator
- `fn except(left_plan: LogicalPlan, right_plan: LogicalPlan, is_all: bool) -> Result<LogicalPlan>` - Process except set operator
- `fn build(self: Self) -> Result<LogicalPlan>` - Build the plan
- `fn join_with_expr_keys<impl Into<Expr>, impl Into<Expr>>(self: Self, right: LogicalPlan, join_type: JoinType, equi_exprs: (Vec<impl Trait>, Vec<impl Trait>), filter: Option<Expr>) -> Result<Self>` - Apply a join with both explicit equijoin and non equijoin predicates.
- `fn unnest_column<impl Into<Column>>(self: Self, column: impl Trait) -> Result<Self>` - Unnest the given column.
- `fn unnest_column_with_options<impl Into<Column>>(self: Self, column: impl Trait, options: UnnestOptions) -> Result<Self>` - Unnest the given column given [`UnnestOptions`]
- `fn unnest_columns_with_options(self: Self, columns: Vec<Column>, options: UnnestOptions) -> Result<Self>` - Unnest the given columns with the given [`UnnestOptions`]

**Trait Implementations:**

- **From**
  - `fn from(plan: Arc<LogicalPlan>) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> LogicalPlanBuilder`
- **From**
  - `fn from(plan: LogicalPlan) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::builder::LogicalPlanBuilderOptions

*Struct*

Options for [`LogicalPlanBuilder`]

**Methods:**

- `fn new() -> Self`
- `fn with_add_implicit_group_by_exprs(self: Self, add: bool) -> Self` - Should the builder add functionally dependent expressions as additional aggregation groupings.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> LogicalPlanBuilderOptions`
- **Default**
  - `fn default() -> LogicalPlanBuilderOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::builder::LogicalTableSource

*Struct*

Basic TableSource implementation intended for use in tests and documentation. It is expected
that users will provide their own TableSource implementations or use DataFusion's
DefaultTableSource.

**Methods:**

- `fn new(table_schema: SchemaRef) -> Self` - Create a new LogicalTableSource
- `fn with_constraints(self: Self, constraints: Constraints) -> Self`

**Trait Implementations:**

- **TableSource**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn constraints(self: &Self) -> Option<&Constraints>`
  - `fn supports_filters_pushdown(self: &Self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>`



## datafusion_expr::logical_plan::builder::UNNAMED_TABLE

*Constant*: `&str`

Default table name for unnamed table



## datafusion_expr::logical_plan::builder::add_group_by_exprs_from_dependencies

*Function*

Add additional "synthetic" group by expressions based on functional
dependencies.

For example, if we are grouping on `[c1]`, and we know from
functional dependencies that column `c1` determines `c2`, this function
adds `c2` to the group by list.

This allows MySQL style selects like
`SELECT col FROM t WHERE pk = 5` if col is unique

```rust
fn add_group_by_exprs_from_dependencies(group_expr: Vec<crate::Expr>, schema: &datafusion_common::DFSchemaRef) -> datafusion_common::Result<Vec<crate::Expr>>
```



## datafusion_expr::logical_plan::builder::build_join_schema

*Function*

Creates a schema for a join operation.
The fields from the left side are first

```rust
fn build_join_schema(left: &datafusion_common::DFSchema, right: &datafusion_common::DFSchema, join_type: &crate::logical_plan::JoinType) -> datafusion_common::Result<datafusion_common::DFSchema>
```



## datafusion_expr::logical_plan::builder::get_struct_unnested_columns

*Function*

```rust
fn get_struct_unnested_columns(col_name: &String, inner_fields: &arrow::datatypes::Fields) -> Vec<datafusion_common::Column>
```



## datafusion_expr::logical_plan::builder::project

*Function*

Create Projection
# Errors
This function errors under any of the following conditions:
* Two or more expressions have the same name
* An invalid expression is used (e.g. a `sort` expression)

```rust
fn project<impl Into<SelectExpr>, impl IntoIterator<Item = impl Into<SelectExpr>>>(plan: crate::logical_plan::LogicalPlan, expr: impl Trait) -> datafusion_common::Result<crate::logical_plan::LogicalPlan>
```



## datafusion_expr::logical_plan::builder::requalify_sides_if_needed

*Function*

(Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
conflict with the columns from the other.
This is especially useful for queries that come as Substrait, since Substrait doesn't currently allow specifying
aliases, neither for columns nor for tables.  DataFusion requires columns to be uniquely identifiable, in some
places (see e.g. DFSchema::check_names).
The function returns:
- The requalified or original left logical plan
- The requalified or original right logical plan
- If a requalification was needed or not

```rust
fn requalify_sides_if_needed(left: LogicalPlanBuilder, right: LogicalPlanBuilder) -> datafusion_common::Result<(LogicalPlanBuilder, LogicalPlanBuilder, bool)>
```



## datafusion_expr::logical_plan::builder::subquery_alias

*Function*

Create a SubqueryAlias to wrap a LogicalPlan.

```rust
fn subquery_alias<impl Into<TableReference>>(plan: crate::logical_plan::LogicalPlan, alias: impl Trait) -> datafusion_common::Result<crate::logical_plan::LogicalPlan>
```



## datafusion_expr::logical_plan::builder::table_scan

*Function*

Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
This is mostly used for testing and documentation.

```rust
fn table_scan<impl Into<TableReference>>(name: Option<impl Trait>, table_schema: &arrow::datatypes::Schema, projection: Option<Vec<usize>>) -> datafusion_common::Result<LogicalPlanBuilder>
```



## datafusion_expr::logical_plan::builder::table_scan_with_filter_and_fetch

*Function*

Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
filters, and inlined fetch.
This is mostly used for testing and documentation.

```rust
fn table_scan_with_filter_and_fetch<impl Into<TableReference>>(name: Option<impl Trait>, table_schema: &arrow::datatypes::Schema, projection: Option<Vec<usize>>, filters: Vec<crate::Expr>, fetch: Option<usize>) -> datafusion_common::Result<LogicalPlanBuilder>
```



## datafusion_expr::logical_plan::builder::table_scan_with_filters

*Function*

Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
and inlined filters.
This is mostly used for testing and documentation.

```rust
fn table_scan_with_filters<impl Into<TableReference>>(name: Option<impl Trait>, table_schema: &arrow::datatypes::Schema, projection: Option<Vec<usize>>, filters: Vec<crate::Expr>) -> datafusion_common::Result<LogicalPlanBuilder>
```



## datafusion_expr::logical_plan::builder::table_source

*Function*

```rust
fn table_source(table_schema: &arrow::datatypes::Schema) -> std::sync::Arc<dyn TableSource>
```



## datafusion_expr::logical_plan::builder::table_source_with_constraints

*Function*

```rust
fn table_source_with_constraints(table_schema: &arrow::datatypes::Schema, constraints: datafusion_common::Constraints) -> std::sync::Arc<dyn TableSource>
```



## datafusion_expr::logical_plan::builder::union

*Function*

Union two [`LogicalPlan`]s.

Constructs the UNION plan, but does not perform type-coercion. Therefore the
subtree expressions will not be properly typed until the optimizer pass.

If a properly typed UNION plan is needed, refer to [`TypeCoercionRewriter::coerce_union`]
or alternatively, merge the union input schema using [`coerce_union_schema`] and
apply the expression rewrite with [`coerce_plan_expr_for_schema`].

[`TypeCoercionRewriter::coerce_union`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/analyzer/type_coercion/struct.TypeCoercionRewriter.html#method.coerce_union
[`coerce_union_schema`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/analyzer/type_coercion/fn.coerce_union_schema.html

```rust
fn union(left_plan: crate::logical_plan::LogicalPlan, right_plan: crate::logical_plan::LogicalPlan) -> datafusion_common::Result<crate::logical_plan::LogicalPlan>
```



## datafusion_expr::logical_plan::builder::union_by_name

*Function*

Like [`union`], but combine rows from different tables by name, rather than
by position.

```rust
fn union_by_name(left_plan: crate::logical_plan::LogicalPlan, right_plan: crate::logical_plan::LogicalPlan) -> datafusion_common::Result<crate::logical_plan::LogicalPlan>
```



## datafusion_expr::logical_plan::builder::unique_field_aliases

*Function*

Returns aliases to make field names unique.

Returns a vector of optional aliases, one per input field. `None` means keep the original name,
`Some(alias)` means rename to the alias to ensure uniqueness.

Used when creating [`SubqueryAlias`] or similar operations that strip table qualifiers but need
to maintain unique column names.

# Example
Input fields: `[a, a, b, b, a, a:1]` ([`DFSchema`] valid when duplicate fields have different qualifiers)
Returns: `[None, Some("a:1"), None, Some("b:1"), Some("a:2"), Some("a:1:1")]`

```rust
fn unique_field_aliases(fields: &arrow::datatypes::Fields) -> Vec<Option<String>>
```



## datafusion_expr::logical_plan::builder::unnest

*Function*

Create a [`LogicalPlan::Unnest`] plan

```rust
fn unnest(input: crate::logical_plan::LogicalPlan, columns: Vec<datafusion_common::Column>) -> datafusion_common::Result<crate::logical_plan::LogicalPlan>
```



## datafusion_expr::logical_plan::builder::unnest_with_options

*Function*

Create a [`LogicalPlan::Unnest`] plan with options
This function receive a list of columns to be unnested
because multiple unnest can be performed on the same column (e.g unnest with different depth)
The new schema will contains post-unnest fields replacing the original field

For example:
Input schema as
```text
+---------------------+-------------------+
| col1                | col2              |
+---------------------+-------------------+
| Struct(INT64,INT32) | List(List(Int64)) |
+---------------------+-------------------+
```



Then unnesting columns with:
- (col1,Struct)
- (col2,List(\[depth=1,depth=2\]))

will generate a new schema as
```text
+---------+---------+---------------------+---------------------+
| col1.c0 | col1.c1 | unnest_col2_depth_1 | unnest_col2_depth_2 |
+---------+---------+---------------------+---------------------+
| Int64   | Int32   | List(Int64)         |  Int64              |
+---------+---------+---------------------+---------------------+
```

```rust
fn unnest_with_options(input: crate::logical_plan::LogicalPlan, columns_to_unnest: Vec<datafusion_common::Column>, options: datafusion_common::UnnestOptions) -> datafusion_common::Result<crate::logical_plan::LogicalPlan>
```



## datafusion_expr::logical_plan::builder::validate_unique_names

*Function*

Errors if one or more expressions have equal names.

```rust
fn validate_unique_names<'a, impl IntoIterator<Item = &'a Expr>>(node_name: &str, expressions: impl Trait) -> datafusion_common::Result<()>
```



## datafusion_expr::logical_plan::builder::wrap_projection_for_join_if_necessary

*Function*

Wrap projection for a plan, if the join keys contains normal expression.

```rust
fn wrap_projection_for_join_if_necessary(join_keys: &[crate::Expr], input: crate::logical_plan::LogicalPlan) -> datafusion_common::Result<(crate::logical_plan::LogicalPlan, Vec<datafusion_common::Column>, bool)>
```



