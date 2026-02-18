**datafusion_sql > unparser > expr**

# Module: unparser::expr

## Contents

**Functions**

- [`expr_to_sql`](#expr_to_sql) - Convert a DataFusion [`Expr`] to [`ast::Expr`]

---

## datafusion_sql::unparser::expr::expr_to_sql

*Function*

Convert a DataFusion [`Expr`] to [`ast::Expr`]

This function is the opposite of [`SqlToRel::sql_to_expr`] and can be used
to, among other things, convert [`Expr`]s to SQL strings. Such strings could
be used to pass filters or other expressions to another SQL engine.

# Errors

Throws an error if [`Expr`] can not be represented by an [`ast::Expr`]

# See Also

* [`Unparser`] for more control over the conversion to SQL
* [`plan_to_sql`] for converting a [`LogicalPlan`] to SQL

# Example
```
use datafusion_expr::{col, lit};
use datafusion_sql::unparser::expr_to_sql;
let expr = col("a").gt(lit(4)); // form an expression `a > 4`
let sql = expr_to_sql(&expr).unwrap(); // convert to ast::Expr, using
assert_eq!(sql.to_string(), "(a > 4)"); // use Display impl for SQL text
```

[`SqlToRel::sql_to_expr`]: crate::planner::SqlToRel::sql_to_expr
[`plan_to_sql`]: crate::unparser::plan_to_sql
[`LogicalPlan`]: datafusion_expr::logical_plan::LogicalPlan

```rust
fn expr_to_sql(expr: &datafusion_expr::Expr) -> datafusion_common::Result<ast::Expr>
```



