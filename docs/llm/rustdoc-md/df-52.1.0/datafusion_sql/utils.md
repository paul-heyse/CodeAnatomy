**datafusion_sql > utils**

# Module: utils

## Contents

**Functions**

- [`window_expr_common_partition_keys`](#window_expr_common_partition_keys) - Given a slice of window expressions sharing the same sort key, find their common partition

**Constants**

- [`UNNEST_PLACEHOLDER`](#unnest_placeholder)

---

## datafusion_sql::utils::UNNEST_PLACEHOLDER

*Constant*: `&str`



## datafusion_sql::utils::window_expr_common_partition_keys

*Function*

Given a slice of window expressions sharing the same sort key, find their common partition
keys.

```rust
fn window_expr_common_partition_keys(window_exprs: &[datafusion_expr::Expr]) -> datafusion_common::Result<&[datafusion_expr::Expr]>
```



