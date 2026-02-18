**datafusion_expr > planner**

# Module: planner

## Contents

**Structs**

- [`PlannedRelation`](#plannedrelation) - Result of planning a relation with [`RelationPlanner`]
- [`RawAggregateExpr`](#rawaggregateexpr) - This structure is used by `AggregateFunctionPlanner` to plan operators with
- [`RawBinaryExpr`](#rawbinaryexpr) - An operator with two arguments to plan
- [`RawDictionaryExpr`](#rawdictionaryexpr) - A Dictionary literal expression `{ key: value, ...}`
- [`RawFieldAccessExpr`](#rawfieldaccessexpr) - An expression with GetFieldAccess to plan
- [`RawWindowExpr`](#rawwindowexpr) - This structure is used by `WindowFunctionPlanner` to plan operators with

**Enums**

- [`PlannerResult`](#plannerresult) - Result of planning a raw expr with [`ExprPlanner`]
- [`RelationPlanning`](#relationplanning) - Result of attempting to plan a relation with extension planners

**Traits**

- [`ContextProvider`](#contextprovider) - Provides the `SQL` query planner meta-data about tables and
- [`ExprPlanner`](#exprplanner) - Customize planning of SQL AST expressions to [`Expr`]s
- [`RelationPlanner`](#relationplanner) - Customize planning SQL table factors to [`LogicalPlan`]s.
- [`RelationPlannerContext`](#relationplannercontext) - Provides utilities for relation planners to interact with DataFusion's SQL
- [`TypePlanner`](#typeplanner) - Customize planning SQL types to DataFusion (Arrow) types.

---

## datafusion_expr::planner::ContextProvider

*Trait*

Provides the `SQL` query planner meta-data about tables and
functions referenced in SQL statements, without a direct dependency on the
`datafusion` Catalog structures such as [`TableProvider`]

[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

**Methods:**

- `get_table_source`: Returns a table by reference, if it exists
- `get_file_type`: Return the type of a file based on its extension (e.g. `.parquet`)
- `get_table_function_source`: Getter for a table function
- `create_cte_work_table`: Provides an intermediate table that is used to store the results of a CTE during execution
- `get_expr_planners`: Return [`ExprPlanner`] extensions for planning expressions
- `get_relation_planners`: Return [`RelationPlanner`] extensions for planning table factors
- `get_type_planner`: Return [`TypePlanner`] extensions for planning data types
- `get_function_meta`: Return the scalar function with a given name, if any
- `get_aggregate_meta`: Return the aggregate function with a given name, if any
- `get_window_meta`: Return the window function with a given name, if any
- `get_variable_type`: Return the system/user-defined variable type, if any
- `get_variable_field`: Return metadata about a system/user-defined variable, if any.
- `options`: Return overall configuration options
- `udf_names`: Return all scalar function names
- `udaf_names`: Return all aggregate function names
- `udwf_names`: Return all window function names



## datafusion_expr::planner::ExprPlanner

*Trait*

Customize planning of SQL AST expressions to [`Expr`]s

**Methods:**

- `plan_binary_op`: Plan the binary operation between two expressions, returns original
- `plan_field_access`: Plan the field access expression, such as `foo.bar`
- `plan_array_literal`: Plan an array literal, such as `[1, 2, 3]`
- `plan_position`: Plan a `POSITION` expression, such as `POSITION(<expr> in <expr>)`
- `plan_dictionary_literal`: Plan a dictionary literal, such as `{ key: value, ...}`
- `plan_extract`: Plan an extract expression, such as`EXTRACT(month FROM foo)`
- `plan_substring`: Plan an substring expression, such as `SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])`
- `plan_struct_literal`: Plans a struct literal, such as  `{'field1' : expr1, 'field2' : expr2, ...}`
- `plan_overlay`: Plans an overlay expression, such as `overlay(str PLACING substr FROM pos [FOR count])`
- `plan_make_map`: Plans a `make_map` expression, such as `make_map(key1, value1, key2, value2, ...)`
- `plan_compound_identifier`: Plans compound identifier such as `db.schema.table` for non-empty nested names
- `plan_any`: Plans `ANY` expression, such as `expr = ANY(array_expr)`
- `plan_aggregate`: Plans aggregate functions, such as `COUNT(<expr>)`
- `plan_window`: Plans window functions, such as `COUNT(<expr>)`



## datafusion_expr::planner::PlannedRelation

*Struct*

Result of planning a relation with [`RelationPlanner`]

**Fields:**
- `plan: crate::logical_plan::LogicalPlan` - The logical plan for the relation
- `alias: Option<sqlparser::ast::TableAlias>` - Optional table alias for the relation

**Methods:**

- `fn new(plan: LogicalPlan, alias: Option<TableAlias>) -> Self` - Create a new `PlannedRelation` with the given plan and alias

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PlannedRelation`



## datafusion_expr::planner::PlannerResult

*Enum*

Result of planning a raw expr with [`ExprPlanner`]

**Generic Parameters:**
- T

**Variants:**
- `Planned(crate::Expr)` - The raw expression was successfully planned as a new [`Expr`]
- `Original(T)` - The raw expression could not be planned, and is returned unmodified

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PlannerResult<T>`



## datafusion_expr::planner::RawAggregateExpr

*Struct*

This structure is used by `AggregateFunctionPlanner` to plan operators with
custom expressions.

**Fields:**
- `func: std::sync::Arc<crate::AggregateUDF>`
- `args: Vec<crate::Expr>`
- `distinct: bool`
- `filter: Option<Box<crate::Expr>>`
- `order_by: Vec<crate::SortExpr>`
- `null_treatment: Option<crate::expr::NullTreatment>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RawAggregateExpr`



## datafusion_expr::planner::RawBinaryExpr

*Struct*

An operator with two arguments to plan

Note `left` and `right` are DataFusion [`Expr`]s but the `op` is the SQL AST
operator.

This structure is used by [`ExprPlanner`] to plan operators with
custom expressions.

**Fields:**
- `op: sqlparser::ast::BinaryOperator`
- `left: crate::Expr`
- `right: crate::Expr`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RawBinaryExpr`



## datafusion_expr::planner::RawDictionaryExpr

*Struct*

A Dictionary literal expression `{ key: value, ...}`

This structure is used by [`ExprPlanner`] to plan operators with
custom expressions.

**Fields:**
- `keys: Vec<crate::Expr>`
- `values: Vec<crate::Expr>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RawDictionaryExpr`



## datafusion_expr::planner::RawFieldAccessExpr

*Struct*

An expression with GetFieldAccess to plan

This structure is used by [`ExprPlanner`] to plan operators with
custom expressions.

**Fields:**
- `field_access: crate::GetFieldAccess`
- `expr: crate::Expr`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RawFieldAccessExpr`



## datafusion_expr::planner::RawWindowExpr

*Struct*

This structure is used by `WindowFunctionPlanner` to plan operators with
custom expressions.

**Fields:**
- `func_def: crate::WindowFunctionDefinition`
- `args: Vec<crate::Expr>`
- `partition_by: Vec<crate::Expr>`
- `order_by: Vec<crate::SortExpr>`
- `window_frame: crate::WindowFrame`
- `filter: Option<Box<crate::Expr>>`
- `null_treatment: Option<crate::expr::NullTreatment>`
- `distinct: bool`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RawWindowExpr`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::planner::RelationPlanner

*Trait*

Customize planning SQL table factors to [`LogicalPlan`]s.

**Methods:**

- `plan_relation`: Plan a table factor into a [`LogicalPlan`].



## datafusion_expr::planner::RelationPlannerContext

*Trait*

Provides utilities for relation planners to interact with DataFusion's SQL
planner.

This trait provides SQL planning utilities specific to relation planning,
such as converting SQL expressions to logical expressions and normalizing
identifiers. It uses composition to provide access to session context via
[`ContextProvider`].

**Methods:**

- `context_provider`: Provides access to the underlying context provider for reading session
- `plan`: Plans the specified relation through the full planner pipeline, starting
- `sql_to_expr`: Converts a SQL expression into a logical expression using the current
- `sql_expr_to_logical_expr`: Converts a SQL expression into a logical expression without DataFusion
- `normalize_ident`: Normalizes an identifier according to session settings.
- `object_name_to_table_reference`: Normalizes a SQL object name into a [`TableReference`].



## datafusion_expr::planner::RelationPlanning

*Enum*

Result of attempting to plan a relation with extension planners

**Variants:**
- `Planned(PlannedRelation)` - The relation was successfully planned by an extension planner
- `Original(sqlparser::ast::TableFactor)` - No extension planner handled the relation, return it for default processing

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::planner::TypePlanner

*Trait*

Customize planning SQL types to DataFusion (Arrow) types.

**Methods:**

- `plan_type`: Plan SQL [`sqlparser::ast::DataType`] to DataFusion [`DataType`]



