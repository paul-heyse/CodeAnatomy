**datafusion_expr > expr_rewriter > order_by**

# Module: expr_rewriter::order_by

## Contents

**Functions**

- [`rewrite_sort_cols_by_aggs`](#rewrite_sort_cols_by_aggs) - Rewrite sort on aggregate expressions to sort on the column of aggregate output

---

## datafusion_expr::expr_rewriter::order_by::rewrite_sort_cols_by_aggs

*Function*

Rewrite sort on aggregate expressions to sort on the column of aggregate output
For example, `max(x)` is written to `col("max(x)")`

```rust
fn rewrite_sort_cols_by_aggs<impl Into<Sort>, impl IntoIterator<Item = impl Into<Sort>>>(sorts: impl Trait, plan: &crate::LogicalPlan) -> datafusion_common::Result<Vec<crate::expr::Sort>>
```



