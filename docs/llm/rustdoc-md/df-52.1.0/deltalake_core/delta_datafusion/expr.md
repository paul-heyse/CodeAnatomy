**deltalake_core > delta_datafusion > expr**

# Module: delta_datafusion::expr

## Contents

**Functions**

- [`fmt_expr_to_sql`](#fmt_expr_to_sql) - Format an `Expr` to a parsable SQL expression
- [`parse_predicate_expression`](#parse_predicate_expression) - Parse a string predicate into an `Expr`

**Constants**

- [`EPOCH_DAYS_FROM_CE`](#epoch_days_from_ce) - Epoch days from ce calendar until 1970-01-01

---

## deltalake_core::delta_datafusion::expr::EPOCH_DAYS_FROM_CE

*Constant*: `i32`

Epoch days from ce calendar until 1970-01-01



## deltalake_core::delta_datafusion::expr::fmt_expr_to_sql

*Function*

Format an `Expr` to a parsable SQL expression

```rust
fn fmt_expr_to_sql(expr: &datafusion::logical_expr::Expr) -> datafusion::common::Result<String, crate::DeltaTableError>
```



## deltalake_core::delta_datafusion::expr::parse_predicate_expression

*Function*

Parse a string predicate into an `Expr`

```rust
fn parse_predicate_expression<impl AsRef<str>>(schema: &datafusion::common::DFSchema, expr: impl Trait, session: &dyn Session) -> crate::DeltaResult<datafusion::logical_expr::Expr>
```



