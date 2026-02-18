**datafusion_sql > unparser**

# Module: unparser

## Contents

**Modules**

- [`ast`](#ast)
- [`dialect`](#dialect)
- [`extension_unparser`](#extension_unparser)

**Structs**

- [`Unparser`](#unparser) - Convert a DataFusion [`Expr`] to [`sqlparser::ast::Expr`]

---

## datafusion_sql::unparser::Unparser

*Struct*

Convert a DataFusion [`Expr`] to [`sqlparser::ast::Expr`]

See [`expr_to_sql`] for background. `Unparser` allows greater control of
the conversion, but with a more complicated API.

To get more human-readable output, see [`Self::with_pretty`]

# Example
```
use datafusion_expr::{col, lit};
use datafusion_sql::unparser::Unparser;
let expr = col("a").gt(lit(4)); // form an expression `a > 4`
let unparser = Unparser::default();
let sql = unparser.expr_to_sql(&expr).unwrap();// convert to AST
// use the Display impl to convert to SQL text
assert_eq!(sql.to_string(), "(a > 4)");
// now convert to pretty sql
let unparser = unparser.with_pretty(true);
let sql = unparser.expr_to_sql(&expr).unwrap();
assert_eq!(sql.to_string(), "a > 4"); // note lack of parenthesis
```

[`Expr`]: datafusion_expr::Expr

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(dialect: &'a dyn Dialect) -> Self`
- `fn with_pretty(self: Self, pretty: bool) -> Self` - Create pretty SQL output, better suited for human consumption
- `fn with_extension_unparsers(self: Self, extension_unparsers: Vec<Arc<dyn UserDefinedLogicalNodeUnparser>>) -> Self` - Add a custom unparser for user defined logical nodes
- `fn expr_to_sql(self: &Self, expr: &Expr) -> Result<ast::Expr>`
- `fn scalar_function_to_sql(self: &Self, func_name: &str, args: &[Expr]) -> Result<ast::Expr>`
- `fn sort_to_sql(self: &Self, sort: &Sort) -> Result<ast::OrderByExpr>`
- `fn col_to_sql(self: &Self, col: &Column) -> Result<ast::Expr>`
- `fn plan_to_sql(self: &Self, plan: &LogicalPlan) -> Result<ast::Statement>`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## Module: ast



## Module: dialect



## Module: extension_unparser



