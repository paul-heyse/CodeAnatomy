**datafusion_sql > planner**

# Module: planner

## Contents

**Structs**

- [`IdentNormalizer`](#identnormalizer) - Ident Normalizer
- [`ParserOptions`](#parseroptions) - SQL parser options
- [`PlannerContext`](#plannercontext) - Struct to store the states used by the Planner. The Planner will leverage the states
- [`SqlToRel`](#sqltorel) - SQL query planner and binder

**Enums**

- [`NullOrdering`](#nullordering) - Represents the null ordering for sorting expressions.

**Functions**

- [`object_name_to_qualifier`](#object_name_to_qualifier) - Construct a WHERE qualifier suitable for e.g. information_schema filtering
- [`object_name_to_table_reference`](#object_name_to_table_reference) - Create a [`TableReference`] after normalizing the specified ObjectName

---

## datafusion_sql::planner::IdentNormalizer

*Struct*

Ident Normalizer

**Methods:**

- `fn new(normalize: bool) -> Self`
- `fn normalize(self: &Self, ident: Ident) -> String`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_sql::planner::NullOrdering

*Enum*

Represents the null ordering for sorting expressions.

**Variants:**
- `NullsMax` - Nulls appear last in ascending order.
- `NullsMin` - Nulls appear first in descending order.
- `NullsFirst` - Nulls appear first.
- `NullsLast` - Nulls appear last.

**Methods:**

- `fn nulls_first(self: &Self, asc: bool) -> bool` - Evaluates the null ordering based on the given ascending flag.

**Traits:** Copy

**Trait Implementations:**

- **FromStr**
  - `fn from_str(s: &str) -> Result<Self>`
- **From**
  - `fn from(s: &str) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> NullOrdering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_sql::planner::ParserOptions

*Struct*

SQL parser options

**Fields:**
- `parse_float_as_decimal: bool` - Whether to parse float as decimal.
- `enable_ident_normalization: bool` - Whether to normalize identifiers.
- `support_varchar_with_length: bool` - Whether to support varchar with length.
- `enable_options_value_normalization: bool` - Whether to normalize options value.
- `collect_spans: bool` - Whether to collect spans
- `map_string_types_to_utf8view: bool` - Whether string types (VARCHAR, CHAR, Text, and String) are mapped to `Utf8View` during SQL planning.
- `default_null_ordering: NullOrdering` - Default null ordering for sorting expressions.

**Methods:**

- `fn new() -> Self` - Creates a new `ParserOptions` instance with default values.
- `fn with_parse_float_as_decimal(self: Self, value: bool) -> Self` - Sets the `parse_float_as_decimal` option.
- `fn with_enable_ident_normalization(self: Self, value: bool) -> Self` - Sets the `enable_ident_normalization` option.
- `fn with_support_varchar_with_length(self: Self, value: bool) -> Self` - Sets the `support_varchar_with_length` option.
- `fn with_map_string_types_to_utf8view(self: Self, value: bool) -> Self` - Sets the `map_string_types_to_utf8view` option.
- `fn with_enable_options_value_normalization(self: Self, value: bool) -> Self` - Sets the `enable_options_value_normalization` option.
- `fn with_collect_spans(self: Self, value: bool) -> Self` - Sets the `collect_spans` option.
- `fn with_default_null_ordering(self: Self, value: NullOrdering) -> Self` - Sets the `default_null_ordering` option.

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(options: &SqlParserOptions) -> Self`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> ParserOptions`



## datafusion_sql::planner::PlannerContext

*Struct*

Struct to store the states used by the Planner. The Planner will leverage the states
to resolve CTEs, Views, subqueries and PREPARE statements. The states include
Common Table Expression (CTE) provided with WITH clause and
Parameter Data Types provided with PREPARE statement and the query schema of the
outer query plan.

# Cloning

Only the `ctes` are truly cloned when the `PlannerContext` is cloned.
This helps resolve scoping issues of CTEs.
By using cloning, a subquery can inherit CTEs from the outer query
and can also define its own private CTEs without affecting the outer query.

**Methods:**

- `fn new() -> Self` - Create an empty PlannerContext
- `fn with_prepare_param_data_types(self: Self, prepare_param_data_types: Vec<FieldRef>) -> Self` - Update the PlannerContext with provided prepare_param_data_types
- `fn outer_query_schema(self: &Self) -> Option<&DFSchema>`
- `fn set_outer_query_schema(self: & mut Self, schema: Option<DFSchemaRef>) -> Option<DFSchemaRef>` - Sets the outer query schema, returning the existing one, if
- `fn set_table_schema(self: & mut Self, schema: Option<DFSchemaRef>) -> Option<DFSchemaRef>`
- `fn table_schema(self: &Self) -> Option<DFSchemaRef>`
- `fn outer_from_schema(self: &Self) -> Option<Arc<DFSchema>>`
- `fn set_outer_from_schema(self: & mut Self, schema: Option<DFSchemaRef>) -> Option<DFSchemaRef>` - Sets the outer FROM schema, returning the existing one, if any
- `fn extend_outer_from_schema(self: & mut Self, schema: &DFSchemaRef) -> Result<()>` - Extends the FROM schema, returning the existing one, if any
- `fn prepare_param_data_types(self: &Self) -> &[FieldRef]` - Return the types of parameters (`$1`, `$2`, etc) if known
- `fn contains_cte(self: &Self, cte_name: &str) -> bool` - Returns true if there is a Common Table Expression (CTE) /
- `fn insert_cte<impl Into<String>>(self: & mut Self, cte_name: impl Trait, plan: LogicalPlan)` - Inserts a LogicalPlan for the Common Table Expression (CTE) /
- `fn get_cte(self: &Self, cte_name: &str) -> Option<&LogicalPlan>` - Return a plan for the Common Table Expression (CTE) / Subquery for the

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PlannerContext`



## datafusion_sql::planner::SqlToRel

*Struct*

SQL query planner and binder

This struct is used to convert a SQL AST into a [`LogicalPlan`].

You can control the behavior of the planner by providing [`ParserOptions`].

It performs the following tasks:

1. Name and type resolution (called "binding" in other systems). This
   phase looks up table and column names using the [`ContextProvider`].
2. Mechanical translation of the AST into a [`LogicalPlan`].

It does not perform type coercion, or perform optimization, which are done
by subsequent passes.

Key interfaces are:
* [`Self::sql_statement_to_plan`]: Convert a statement
  (e.g. `SELECT ...`) into a [`LogicalPlan`]
* [`Self::sql_to_expr`]: Convert an expression (e.g. `1 + 2`) into an [`Expr`]

**Generic Parameters:**
- 'a
- S

**Methods:**

- `fn new(context_provider: &'a S) -> Self` - Create a new query planner.
- `fn new_with_options(context_provider: &'a S, options: ParserOptions) -> Self` - Create a new query planner with the given parser options.
- `fn build_schema(self: &Self, columns: Vec<SQLColumnDef>) -> Result<Schema>`
- `fn sql_to_expr_with_alias(self: &Self, sql: SQLExprWithAlias, schema: &DFSchema, planner_context: & mut PlannerContext) -> Result<Expr>`
- `fn sql_to_expr(self: &Self, sql: SQLExpr, schema: &DFSchema, planner_context: & mut PlannerContext) -> Result<Expr>` - Generate a relational expression from a SQL expression
- `fn statement_to_plan(self: &Self, statement: DFStatement) -> Result<LogicalPlan>` - Generate a logical plan from an DataFusion SQL statement
- `fn sql_statement_to_plan(self: &Self, statement: Statement) -> Result<LogicalPlan>` - Generate a logical plan from an SQL statement
- `fn sql_statement_to_plan_with_context(self: &Self, statement: Statement, planner_context: & mut PlannerContext) -> Result<LogicalPlan>` - Generate a logical plan from an SQL statement
- `fn new_constraint_from_table_constraints(self: &Self, constraints: &[TableConstraint], df_schema: &DFSchemaRef) -> Result<Constraints>` - Convert each [TableConstraint] to corresponding [Constraint]



## datafusion_sql::planner::object_name_to_qualifier

*Function*

Construct a WHERE qualifier suitable for e.g. information_schema filtering
from the provided object identifiers (catalog, schema and table names).

```rust
fn object_name_to_qualifier(sql_table_name: &sqlparser::ast::ObjectName, enable_normalization: bool) -> datafusion_common::Result<String>
```



## datafusion_sql::planner::object_name_to_table_reference

*Function*

Create a [`TableReference`] after normalizing the specified ObjectName

Examples
```text
['foo']          -> Bare { table: "foo" }
['"foo.bar"]]    -> Bare { table: "foo.bar" }
['foo', 'Bar']   -> Partial { schema: "foo", table: "bar" } <-- note lower case "bar"
['foo', 'bar']   -> Partial { schema: "foo", table: "bar" }
['foo', '"Bar"'] -> Partial { schema: "foo", table: "Bar" }
```

```rust
fn object_name_to_table_reference(object_name: sqlparser::ast::ObjectName, enable_normalization: bool) -> datafusion_common::Result<datafusion_common::TableReference>
```



