**datafusion_sql > parser**

# Module: parser

## Contents

**Structs**

- [`CopyToStatement`](#copytostatement) - DataFusion extension DDL for `COPY`
- [`CreateExternalTable`](#createexternaltable) - DataFusion extension DDL for `CREATE EXTERNAL TABLE`
- [`DFParser`](#dfparser) - DataFusion SQL Parser based on [`sqlparser`]
- [`DFParserBuilder`](#dfparserbuilder) - Builder for [`DFParser`]
- [`ExplainStatement`](#explainstatement) - DataFusion specific `EXPLAIN`

**Enums**

- [`CopyToSource`](#copytosource)
- [`ResetStatement`](#resetstatement) - DataFusion extension for `RESET`
- [`Statement`](#statement) - DataFusion SQL Statement.

---

## datafusion_sql::parser::CopyToSource

*Enum*

**Variants:**
- `Relation(sqlparser::ast::ObjectName)` - `COPY <table> TO ...`
- `Query(Box<sqlparser::ast::Query>)` - COPY (...query...) TO ...

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CopyToSource`
- **PartialEq**
  - `fn eq(self: &Self, other: &CopyToSource) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_sql::parser::CopyToStatement

*Struct*

DataFusion extension DDL for `COPY`

# Syntax:

```text
COPY <table_name | (<query>)>
TO
<destination_url>
(key_value_list)
```

# Examples

```sql
COPY lineitem  TO 'lineitem'
STORED AS PARQUET (
  partitions 16,
  row_group_limit_rows 100000,
  row_group_limit_bytes 200000
)

COPY (SELECT l_orderkey from lineitem) to 'lineitem.parquet';
```

**Fields:**
- `source: CopyToSource` - From where the data comes from
- `target: String` - The URL to where the data is heading
- `partitioned_by: Vec<String>` - Partition keys
- `stored_as: Option<String>` - File type (Parquet, NDJSON, CSV etc.)
- `options: Vec<(String, sqlparser::ast::Value)>` - Target specific options

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CopyToStatement`
- **PartialEq**
  - `fn eq(self: &Self, other: &CopyToStatement) -> bool`



## datafusion_sql::parser::CreateExternalTable

*Struct*

DataFusion extension DDL for `CREATE EXTERNAL TABLE`

Syntax:

```text
CREATE
[ OR REPLACE ]
EXTERNAL TABLE
[ IF NOT EXISTS ]
<TABLE_NAME>[ (<column_definition>) ]
STORED AS <file_type>
[ PARTITIONED BY (<column_definition list> | <column list>) ]
[ WITH ORDER (<ordered column list>)
[ OPTIONS (<key_value_list>) ]
LOCATION <literal>

<column_definition> := (<column_name> <data_type>, ...)

<column_list> := (<column_name>, ...)

<ordered_column_list> := (<column_name> <sort_clause>, ...)

<key_value_list> := (<literal> <literal, <literal> <literal>, ...)
```

**Fields:**
- `name: sqlparser::ast::ObjectName` - Table name
- `columns: Vec<sqlparser::ast::ColumnDef>` - Optional schema
- `file_type: String` - File type (Parquet, NDJSON, CSV, etc)
- `location: String` - Path to file
- `table_partition_cols: Vec<String>` - Partition Columns
- `order_exprs: Vec<Vec<sqlparser::ast::OrderByExpr>>` - Ordered expressions
- `if_not_exists: bool` - Option to not error if table already exists
- `or_replace: bool` - Option to replace table content if table already exists
- `temporary: bool` - Whether the table is a temporary table
- `unbounded: bool` - Infinite streams?
- `options: Vec<(String, sqlparser::ast::Value)>` - Table(provider) specific options
- `constraints: Vec<sqlparser::ast::TableConstraint>` - A table-level constraint

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> CreateExternalTable`
- **PartialEq**
  - `fn eq(self: &Self, other: &CreateExternalTable) -> bool`



## datafusion_sql::parser::DFParser

*Struct*

DataFusion SQL Parser based on [`sqlparser`]

Parses DataFusion's SQL dialect, often delegating to [`sqlparser`]'s [`Parser`].

DataFusion mostly follows existing SQL dialects via
`sqlparser`. However, certain statements such as `COPY` and
`CREATE EXTERNAL TABLE` have special syntax in DataFusion. See
[`Statement`] for a list of this special syntax

**Generic Parameters:**
- 'a

**Fields:**
- `parser: sqlparser::parser::Parser<'a>`

**Methods:**

- `fn new(sql: &'a str) -> Result<Self, DataFusionError>`
- `fn new_with_dialect(sql: &'a str, dialect: &'a dyn Dialect) -> Result<Self, DataFusionError>`
- `fn parse_sql(sql: &'a str) -> Result<VecDeque<Statement>, DataFusionError>` - Parse a sql string into one or [`Statement`]s using the
- `fn parse_sql_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<VecDeque<Statement>, DataFusionError>` - Parse a SQL string and produce one or more [`Statement`]s with
- `fn parse_sql_into_expr(sql: &str) -> Result<ExprWithAlias, DataFusionError>`
- `fn parse_sql_into_expr_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<ExprWithAlias, DataFusionError>`
- `fn parse_statements(self: & mut Self) -> Result<VecDeque<Statement>, DataFusionError>` - Parse a sql string into one or [`Statement`]s
- `fn parse_statement(self: & mut Self) -> Result<Statement, DataFusionError>` - Parse a new expression
- `fn parse_expr(self: & mut Self) -> Result<ExprWithAlias, DataFusionError>`
- `fn parse_into_expr(self: & mut Self) -> Result<ExprWithAlias, DataFusionError>` - Parses the entire SQL string into an expression.
- `fn parse_copy(self: & mut Self) -> Result<Statement, DataFusionError>` - Parse a SQL `COPY TO` statement
- `fn parse_option_key(self: & mut Self) -> Result<String, DataFusionError>` - Parse the next token as a key name for an option list
- `fn parse_option_value(self: & mut Self) -> Result<Value, DataFusionError>` - Parse the next token as a value for an option list
- `fn parse_explain(self: & mut Self) -> Result<Statement, DataFusionError>` - Parse a SQL `EXPLAIN`
- `fn parse_reset(self: & mut Self) -> Result<Statement, DataFusionError>` - Parse a SQL `RESET`
- `fn parse_explain_format(self: & mut Self) -> Result<Option<String>, DataFusionError>`
- `fn parse_create(self: & mut Self) -> Result<Statement, DataFusionError>` - Parse a SQL `CREATE` statement handling `CREATE EXTERNAL TABLE`
- `fn parse_order_by_exprs(self: & mut Self) -> Result<Vec<OrderByExpr>, DataFusionError>` - Parse the ordering clause of a `CREATE EXTERNAL TABLE` SQL statement
- `fn parse_order_by_expr(self: & mut Self) -> Result<OrderByExpr, DataFusionError>` - Parse an ORDER BY sub-expression optionally followed by ASC or DESC.



## datafusion_sql::parser::DFParserBuilder

*Struct*

Builder for [`DFParser`]

# Example: Create and Parse SQL statements
```
# use datafusion_sql::parser::DFParserBuilder;
# use datafusion_common::Result;
# fn test() -> Result<()> {
let mut parser = DFParserBuilder::new("SELECT * FROM foo; SELECT 1 + 2").build()?;
// parse the SQL into DFStatements
let statements = parser.parse_statements()?;
assert_eq!(statements.len(), 2);
# Ok(())
# }
```

# Example: Create and Parse expression with a different dialect
```
# use datafusion_sql::parser::DFParserBuilder;
# use datafusion_common::Result;
# use datafusion_sql::sqlparser::dialect::MySqlDialect;
# use datafusion_sql::sqlparser::ast::Expr;
# fn test() -> Result<()> {
let dialect = MySqlDialect {}; // Parse using MySQL dialect
let mut parser = DFParserBuilder::new("1 + 2")
    .with_dialect(&dialect)
    .build()?;
// parse 1+2 into an sqlparser::ast::Expr
let res = parser.parse_expr()?;
assert!(matches!(res.expr, Expr::BinaryOp { .. }));
# Ok(())
# }
```

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(sql: &'a str) -> Self` - Create a new parser builder for the specified tokens using the
- `fn with_dialect(self: Self, dialect: &'a dyn Dialect) -> Self` - Adjust the parser builder's dialect. Defaults to [`GenericDialect`]
- `fn with_recursion_limit(self: Self, recursion_limit: usize) -> Self` - Adjust the recursion limit of sql parsing.  Defaults to 50
- `fn build(self: Self) -> Result<DFParser<'a>, DataFusionError>`



## datafusion_sql::parser::ExplainStatement

*Struct*

DataFusion specific `EXPLAIN`

Syntax:
```sql
EXPLAIN <ANALYZE> <VERBOSE> [FORMAT format] statement
```

**Fields:**
- `analyze: bool` - `EXPLAIN ANALYZE ..`
- `verbose: bool` - `EXPLAIN .. VERBOSE ..`
- `format: Option<String>` - `EXPLAIN .. FORMAT `
- `statement: Box<Statement>` - The statement to analyze. Note this is a DataFusion [`Statement`] (not a

**Traits:** Eq

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExplainStatement) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> ExplainStatement`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_sql::parser::ResetStatement

*Enum*

DataFusion extension for `RESET`

**Variants:**
- `Variable(sqlparser::ast::ObjectName)` - Reset a single configuration variable (stored as provided)

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ResetStatement`
- **PartialEq**
  - `fn eq(self: &Self, other: &ResetStatement) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_sql::parser::Statement

*Enum*

DataFusion SQL Statement.

This can either be a [`Statement`] from [`sqlparser`] from a
standard SQL dialect, or a DataFusion extension such as `CREATE
EXTERNAL TABLE`. See [`DFParser`] for more information.

[`Statement`]: sqlparser::ast::Statement

**Variants:**
- `Statement(Box<sqlparser::ast::Statement>)` - ANSI SQL AST node (from sqlparser-rs)
- `CreateExternalTable(CreateExternalTable)` - Extension: `CREATE EXTERNAL TABLE`
- `CopyTo(CopyToStatement)` - Extension: `COPY TO`
- `Explain(ExplainStatement)` - EXPLAIN for extensions
- `Reset(ResetStatement)` - Extension: `RESET`

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Statement`
- **PartialEq**
  - `fn eq(self: &Self, other: &Statement) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`



