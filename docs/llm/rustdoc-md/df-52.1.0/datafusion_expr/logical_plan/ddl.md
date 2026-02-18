**datafusion_expr > logical_plan > ddl**

# Module: logical_plan::ddl

## Contents

**Structs**

- [`CreateCatalog`](#createcatalog) - Creates a catalog (aka "Database").
- [`CreateCatalogSchema`](#createcatalogschema) - Creates a schema.
- [`CreateExternalTable`](#createexternaltable) - Creates an external table.
- [`CreateExternalTableBuilder`](#createexternaltablebuilder) - Builder for [`CreateExternalTable`] that provides a fluent API for construction.
- [`CreateFunction`](#createfunction) - Arguments passed to the `CREATE FUNCTION` statement
- [`CreateFunctionBody`](#createfunctionbody) - Part of the `CREATE FUNCTION` statement
- [`CreateIndex`](#createindex)
- [`CreateMemoryTable`](#creatememorytable) - Creates an in memory table.
- [`CreateView`](#createview) - Creates a view.
- [`DropCatalogSchema`](#dropcatalogschema) - Drops a schema
- [`DropFunction`](#dropfunction)
- [`DropTable`](#droptable) - Drops a table.
- [`DropView`](#dropview) - Drops a view.
- [`OperateFunctionArg`](#operatefunctionarg) - Part of the `CREATE FUNCTION` statement

**Enums**

- [`DdlStatement`](#ddlstatement) - Various types of DDL  (CREATE / DROP) catalog manipulation

---

## datafusion_expr::logical_plan::ddl::CreateCatalog

*Struct*

Creates a catalog (aka "Database").

**Fields:**
- `catalog_name: String` - The catalog name
- `if_not_exists: bool` - Do nothing (except issuing a notice) if a schema with the same name already exists
- `schema: datafusion_common::DFSchemaRef` - Empty schema

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CreateCatalog`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateCatalog) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::ddl::CreateCatalogSchema

*Struct*

Creates a schema.

**Fields:**
- `schema_name: String` - The table schema
- `if_not_exists: bool` - Do nothing (except issuing a notice) if a schema with the same name already exists
- `schema: datafusion_common::DFSchemaRef` - Empty schema

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CreateCatalogSchema`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateCatalogSchema) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::ddl::CreateExternalTable

*Struct*

Creates an external table.

**Fields:**
- `schema: datafusion_common::DFSchemaRef` - The table schema
- `name: datafusion_common::TableReference` - The table name
- `location: String` - The physical location
- `file_type: String` - The file type of physical file
- `table_partition_cols: Vec<String>` - Partition Columns
- `if_not_exists: bool` - Option to not error if table already exists
- `or_replace: bool` - Option to replace table content if table already exists
- `temporary: bool` - Whether the table is a temporary table
- `definition: Option<String>` - SQL used to create the table, if available
- `order_exprs: Vec<Vec<crate::expr::Sort>>` - Order expressions supplied by user
- `unbounded: bool` - Whether the table is an infinite streams
- `options: std::collections::HashMap<String, String>` - Table(provider) specific options
- `constraints: datafusion_common::Constraints` - The list of constraints in the schema, such as primary key, unique, etc.
- `column_defaults: std::collections::HashMap<String, crate::Expr>` - Default values for columns

**Methods:**

- `fn builder<impl Into<TableReference>, impl Into<String>, impl Into<String>>(name: impl Trait, location: impl Trait, file_type: impl Trait, schema: DFSchemaRef) -> CreateExternalTableBuilder` - Creates a builder for [`CreateExternalTable`] with required fields.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CreateExternalTable`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateExternalTable) -> bool`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::ddl::CreateExternalTableBuilder

*Struct*

Builder for [`CreateExternalTable`] that provides a fluent API for construction.

Created via [`CreateExternalTable::builder`].



## datafusion_expr::logical_plan::ddl::CreateFunction

*Struct*

Arguments passed to the `CREATE FUNCTION` statement

These statements are turned into executable functions using [`FunctionFactory`]

# Notes

This structure purposely mirrors the structure in sqlparser's
[`sqlparser::ast::Statement::CreateFunction`], but does not use it directly
to avoid a dependency on sqlparser in the core crate.


[`FunctionFactory`]: https://docs.rs/datafusion/latest/datafusion/execution/context/trait.FunctionFactory.html

**Fields:**
- `or_replace: bool`
- `temporary: bool`
- `name: String`
- `args: Option<Vec<OperateFunctionArg>>`
- `return_type: Option<arrow::datatypes::DataType>`
- `params: CreateFunctionBody`
- `schema: datafusion_common::DFSchemaRef` - Dummy schema

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &CreateFunction) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> CreateFunction`



## datafusion_expr::logical_plan::ddl::CreateFunctionBody

*Struct*

Part of the `CREATE FUNCTION` statement

See [`CreateFunction`] for details

**Fields:**
- `language: Option<sqlparser::ast::Ident>` - LANGUAGE lang_name
- `behavior: Option<crate::Volatility>` - IMMUTABLE | STABLE | VOLATILE
- `function_body: Option<crate::Expr>` - RETURN or AS function body

**Traits:** Eq

**Trait Implementations:**

- **TreeNodeContainer**
  - `fn apply_elements<F>(self: &'a Self, f: F) -> Result<TreeNodeRecursion>`
  - `fn map_elements<F>(self: Self, f: F) -> Result<Transformed<Self>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CreateFunctionBody`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateFunctionBody) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &CreateFunctionBody) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::ddl::CreateIndex

*Struct*

**Fields:**
- `name: Option<String>`
- `table: datafusion_common::TableReference`
- `using: Option<String>`
- `columns: Vec<crate::SortExpr>`
- `unique: bool`
- `if_not_exists: bool`
- `schema: datafusion_common::DFSchemaRef`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> CreateIndex`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateIndex) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::ddl::CreateMemoryTable

*Struct*

Creates an in memory table.

**Fields:**
- `name: datafusion_common::TableReference` - The table name
- `constraints: datafusion_common::Constraints` - The list of constraints in the schema, such as primary key, unique, etc.
- `input: std::sync::Arc<crate::LogicalPlan>` - The logical plan
- `if_not_exists: bool` - Option to not error if table already exists
- `or_replace: bool` - Option to replace table content if table already exists
- `column_defaults: Vec<(String, crate::Expr)>` - Default values for columns
- `temporary: bool` - Whether the table is `TableType::Temporary`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &CreateMemoryTable) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CreateMemoryTable`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateMemoryTable) -> bool`



## datafusion_expr::logical_plan::ddl::CreateView

*Struct*

Creates a view.

**Fields:**
- `name: datafusion_common::TableReference` - The table name
- `input: std::sync::Arc<crate::LogicalPlan>` - The logical plan
- `or_replace: bool` - Option to not error if table already exists
- `definition: Option<String>` - SQL used to create the view, if available
- `temporary: bool` - Whether the view is ephemeral

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &CreateView) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CreateView`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateView) -> bool`



## datafusion_expr::logical_plan::ddl::DdlStatement

*Enum*

Various types of DDL  (CREATE / DROP) catalog manipulation

**Variants:**
- `CreateExternalTable(CreateExternalTable)` - Creates an external table.
- `CreateMemoryTable(CreateMemoryTable)` - Creates an in memory table.
- `CreateView(CreateView)` - Creates a new view.
- `CreateCatalogSchema(CreateCatalogSchema)` - Creates a new catalog schema.
- `CreateCatalog(CreateCatalog)` - Creates a new catalog (aka "Database").
- `CreateIndex(CreateIndex)` - Creates a new index.
- `DropTable(DropTable)` - Drops a table.
- `DropView(DropView)` - Drops a view.
- `DropCatalogSchema(DropCatalogSchema)` - Drops a catalog schema
- `CreateFunction(CreateFunction)` - Create function statement
- `DropFunction(DropFunction)` - Drop function statement

**Methods:**

- `fn schema(self: &Self) -> &DFSchemaRef` - Get a reference to the logical plan's schema
- `fn name(self: &Self) -> &str` - Return a descriptive string describing the type of this
- `fn inputs(self: &Self) -> Vec<&LogicalPlan>` - Return all inputs for this plan
- `fn display(self: &Self) -> impl Trait` - Return a `format`able structure with the a human readable

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DdlStatement`
- **PartialEq**
  - `fn eq(self: &Self, other: &DdlStatement) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &DdlStatement) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::ddl::DropCatalogSchema

*Struct*

Drops a schema

**Fields:**
- `name: datafusion_common::SchemaReference` - The schema name
- `if_exists: bool` - If the schema exists
- `cascade: bool` - Whether drop should cascade
- `schema: datafusion_common::DFSchemaRef` - Dummy schema

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DropCatalogSchema`
- **PartialEq**
  - `fn eq(self: &Self, other: &DropCatalogSchema) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`



## datafusion_expr::logical_plan::ddl::DropFunction

*Struct*

**Fields:**
- `name: String`
- `if_exists: bool`
- `schema: datafusion_common::DFSchemaRef`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Clone**
  - `fn clone(self: &Self) -> DropFunction`
- **PartialEq**
  - `fn eq(self: &Self, other: &DropFunction) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::ddl::DropTable

*Struct*

Drops a table.

**Fields:**
- `name: datafusion_common::TableReference` - The table name
- `if_exists: bool` - If the table exists
- `schema: datafusion_common::DFSchemaRef` - Dummy schema

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &DropTable) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DropTable`



## datafusion_expr::logical_plan::ddl::DropView

*Struct*

Drops a view.

**Fields:**
- `name: datafusion_common::TableReference` - The view name
- `if_exists: bool` - If the view exists
- `schema: datafusion_common::DFSchemaRef` - Dummy schema

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DropView`
- **PartialEq**
  - `fn eq(self: &Self, other: &DropView) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Self) -> Option<Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::ddl::OperateFunctionArg

*Struct*

Part of the `CREATE FUNCTION` statement

See [`CreateFunction`] for details

**Fields:**
- `name: Option<sqlparser::ast::Ident>`
- `data_type: arrow::datatypes::DataType`
- `default_expr: Option<crate::Expr>`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> OperateFunctionArg`
- **PartialEq**
  - `fn eq(self: &Self, other: &OperateFunctionArg) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &OperateFunctionArg) -> $crate::option::Option<$crate::cmp::Ordering>`
- **TreeNodeContainer**
  - `fn apply_elements<F>(self: &'a Self, f: F) -> Result<TreeNodeRecursion>`
  - `fn map_elements<F>(self: Self, f: F) -> Result<Transformed<Self>>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



