**datafusion_sql > unparser > plan**

# Module: unparser::plan

## Contents

**Functions**

- [`plan_to_sql`](#plan_to_sql) - Convert a DataFusion [`LogicalPlan`] to [`ast::Statement`]

---

## datafusion_sql::unparser::plan::plan_to_sql

*Function*

Convert a DataFusion [`LogicalPlan`] to [`ast::Statement`]

This function is the opposite of [`SqlToRel::sql_statement_to_plan`] and can
be used to, among other things, to convert `LogicalPlan`s to SQL strings.

# Errors

This function returns an error if the plan cannot be converted to SQL.

# See Also

* [`expr_to_sql`] for converting [`Expr`], a single expression to SQL

# Example
```
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_expr::{col, logical_plan::table_scan};
use datafusion_sql::unparser::plan_to_sql;
let schema = Schema::new(vec![
    Field::new("id", DataType::Utf8, false),
    Field::new("value", DataType::Utf8, false),
]);
// Scan 'table' and select columns 'id' and 'value'
let plan = table_scan(Some("table"), &schema, None)
    .unwrap()
    .project(vec![col("id"), col("value")])
    .unwrap()
    .build()
    .unwrap();
// convert to AST
let sql = plan_to_sql(&plan).unwrap();
// use the Display impl to convert to SQL text
assert_eq!(
    sql.to_string(),
    "SELECT \"table\".id, \"table\".\"value\" FROM \"table\""
)
```

[`SqlToRel::sql_statement_to_plan`]: crate::planner::SqlToRel::sql_statement_to_plan
[`expr_to_sql`]: crate::unparser::expr_to_sql

```rust
fn plan_to_sql(plan: &datafusion_expr::LogicalPlan) -> datafusion_common::Result<ast::Statement>
```



