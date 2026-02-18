**datafusion_sql > unparser > ast**

# Module: unparser::ast

## Contents

**Structs**

- [`DerivedRelationBuilder`](#derivedrelationbuilder)
- [`QueryBuilder`](#querybuilder)
- [`RelationBuilder`](#relationbuilder)
- [`SelectBuilder`](#selectbuilder)
- [`TableRelationBuilder`](#tablerelationbuilder)
- [`TableWithJoinsBuilder`](#tablewithjoinsbuilder)
- [`UninitializedFieldError`](#uninitializedfielderror) - Runtime error when a `build()` method is called and one or more required fields
- [`UnnestRelationBuilder`](#unnestrelationbuilder)

**Enums**

- [`BuilderError`](#buildererror)

---

## datafusion_sql::unparser::ast::BuilderError

*Enum*

**Variants:**
- `UninitializedField(&'static str)`
- `ValidationError(String)`

**Traits:** Error

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(s: String) -> Self`
- **From**
  - `fn from(s: UninitializedFieldError) -> Self`



## datafusion_sql::unparser::ast::DerivedRelationBuilder

*Struct*

**Methods:**

- `fn lateral(self: & mut Self, value: bool) -> & mut Self`
- `fn subquery(self: & mut Self, value: Box<ast::Query>) -> & mut Self`
- `fn alias(self: & mut Self, value: Option<ast::TableAlias>) -> & mut Self`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DerivedRelationBuilder`
- **Default**
  - `fn default() -> Self`



## datafusion_sql::unparser::ast::QueryBuilder

*Struct*

**Methods:**

- `fn with(self: & mut Self, value: Option<ast::With>) -> & mut Self`
- `fn body(self: & mut Self, value: Box<ast::SetExpr>) -> & mut Self`
- `fn take_body(self: & mut Self) -> Option<Box<ast::SetExpr>>`
- `fn order_by(self: & mut Self, value: OrderByKind) -> & mut Self`
- `fn limit(self: & mut Self, value: Option<ast::Expr>) -> & mut Self`
- `fn limit_by(self: & mut Self, value: Vec<ast::Expr>) -> & mut Self`
- `fn offset(self: & mut Self, value: Option<ast::Offset>) -> & mut Self`
- `fn fetch(self: & mut Self, value: Option<ast::Fetch>) -> & mut Self`
- `fn locks(self: & mut Self, value: Vec<ast::LockClause>) -> & mut Self`
- `fn for_clause(self: & mut Self, value: Option<ast::ForClause>) -> & mut Self`
- `fn distinct_union(self: & mut Self) -> & mut Self`
- `fn is_distinct_union(self: &Self) -> bool`
- `fn build(self: &Self) -> Result<ast::Query, BuilderError>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> QueryBuilder`
- **Default**
  - `fn default() -> Self`



## datafusion_sql::unparser::ast::RelationBuilder

*Struct*

**Methods:**

- `fn has_relation(self: &Self) -> bool`
- `fn table(self: & mut Self, value: TableRelationBuilder) -> & mut Self`
- `fn derived(self: & mut Self, value: DerivedRelationBuilder) -> & mut Self`
- `fn unnest(self: & mut Self, value: UnnestRelationBuilder) -> & mut Self`
- `fn empty(self: & mut Self) -> & mut Self`
- `fn alias(self: & mut Self, value: Option<ast::TableAlias>) -> & mut Self`
- `fn build(self: &Self) -> Result<Option<ast::TableFactor>, BuilderError>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> RelationBuilder`
- **Default**
  - `fn default() -> Self`



## datafusion_sql::unparser::ast::SelectBuilder

*Struct*

**Methods:**

- `fn distinct(self: & mut Self, value: Option<ast::Distinct>) -> & mut Self`
- `fn top(self: & mut Self, value: Option<ast::Top>) -> & mut Self`
- `fn projection(self: & mut Self, value: Vec<ast::SelectItem>) -> & mut Self`
- `fn pop_projections(self: & mut Self) -> Vec<ast::SelectItem>`
- `fn already_projected(self: &Self) -> bool` - Returns true if a projection has been explicitly set via `projection()`.
- `fn into(self: & mut Self, value: Option<ast::SelectInto>) -> & mut Self`
- `fn from(self: & mut Self, value: Vec<TableWithJoinsBuilder>) -> & mut Self`
- `fn push_from(self: & mut Self, value: TableWithJoinsBuilder) -> & mut Self`
- `fn pop_from(self: & mut Self) -> Option<TableWithJoinsBuilder>`
- `fn lateral_views(self: & mut Self, value: Vec<ast::LateralView>) -> & mut Self`
- `fn replace_mark(self: & mut Self, existing_expr: &ast::Expr, value: &ast::Expr) -> & mut Self` - Replaces the selection with a new value.
- `fn selection(self: & mut Self, value: Option<ast::Expr>) -> & mut Self`
- `fn group_by(self: & mut Self, value: ast::GroupByExpr) -> & mut Self`
- `fn cluster_by(self: & mut Self, value: Vec<ast::Expr>) -> & mut Self`
- `fn distribute_by(self: & mut Self, value: Vec<ast::Expr>) -> & mut Self`
- `fn sort_by(self: & mut Self, value: Vec<ast::OrderByExpr>) -> & mut Self`
- `fn having(self: & mut Self, value: Option<ast::Expr>) -> & mut Self`
- `fn named_window(self: & mut Self, value: Vec<ast::NamedWindowDefinition>) -> & mut Self`
- `fn qualify(self: & mut Self, value: Option<ast::Expr>) -> & mut Self`
- `fn value_table_mode(self: & mut Self, value: Option<ast::ValueTableMode>) -> & mut Self`
- `fn build(self: &Self) -> Result<ast::Select, BuilderError>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> SelectBuilder`
- **Default**
  - `fn default() -> Self`



## datafusion_sql::unparser::ast::TableRelationBuilder

*Struct*

**Methods:**

- `fn name(self: & mut Self, value: ast::ObjectName) -> & mut Self`
- `fn alias(self: & mut Self, value: Option<ast::TableAlias>) -> & mut Self`
- `fn args(self: & mut Self, value: Option<Vec<ast::FunctionArg>>) -> & mut Self`
- `fn with_hints(self: & mut Self, value: Vec<ast::Expr>) -> & mut Self`
- `fn version(self: & mut Self, value: Option<ast::TableVersion>) -> & mut Self`
- `fn partitions(self: & mut Self, value: Vec<ast::Ident>) -> & mut Self`
- `fn index_hints(self: & mut Self, value: Vec<ast::TableIndexHints>) -> & mut Self`
- `fn build(self: &Self) -> Result<ast::TableFactor, BuilderError>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TableRelationBuilder`
- **Default**
  - `fn default() -> Self`



## datafusion_sql::unparser::ast::TableWithJoinsBuilder

*Struct*

**Methods:**

- `fn relation(self: & mut Self, value: RelationBuilder) -> & mut Self`
- `fn joins(self: & mut Self, value: Vec<ast::Join>) -> & mut Self`
- `fn push_join(self: & mut Self, value: ast::Join) -> & mut Self`
- `fn build(self: &Self) -> Result<Option<ast::TableWithJoins>, BuilderError>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> TableWithJoinsBuilder`



## datafusion_sql::unparser::ast::UninitializedFieldError

*Struct*

Runtime error when a `build()` method is called and one or more required fields
do not have a value.

**Tuple Struct**: `()`

**Methods:**

- `fn new(field_name: &'static str) -> Self` - Create a new `UninitializedFieldError` for the specified field name.
- `fn field_name(self: &Self) -> &'static str` - Get the name of the first-declared field that wasn't initialized

**Traits:** Error

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(field_name: &'static str) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> UninitializedFieldError`



## datafusion_sql::unparser::ast::UnnestRelationBuilder

*Struct*

**Fields:**
- `alias: Option<ast::TableAlias>`
- `array_exprs: Vec<ast::Expr>`

**Methods:**

- `fn alias(self: & mut Self, value: Option<ast::TableAlias>) -> & mut Self`
- `fn array_exprs(self: & mut Self, value: Vec<ast::Expr>) -> & mut Self`
- `fn with_offset(self: & mut Self, value: bool) -> & mut Self`
- `fn with_offset_alias(self: & mut Self, value: Option<ast::Ident>) -> & mut Self`
- `fn with_ordinality(self: & mut Self, value: bool) -> & mut Self`
- `fn build(self: &Self) -> Result<ast::TableFactor, BuilderError>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> UnnestRelationBuilder`
- **Default**
  - `fn default() -> Self`



