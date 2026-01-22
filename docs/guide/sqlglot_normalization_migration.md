# SQLGlot Normalization Migration

## Summary

The canonical SQLGlot pipeline now uses `optimize()` via `compile_expr`. Existing
call sites may still use `normalize_expr`, but new code should route through the
compile pipeline and policy-aware emission helpers.

## What Changed

- Canonicalization uses `compile_expr` with `SqlGlotCompileOptions`.
- `normalize_expr` is a wrapper around the compile pipeline for compatibility.
- SQL emission should use `sqlglot_emit` or `sqlglot_sql` with a resolved policy.
- Parameterized SQL should be parsed with `parse_sql(..., preserve_params=True)`
  and bound via `bind_params`.

## Recommended Usage

```python
from sqlglot_tools.optimizer import (
    ParseSqlOptions,
    SqlGlotCompileOptions,
    compile_expr,
    parse_sql,
    resolve_sqlglot_policy,
    sqlglot_emit,
)

policy = resolve_sqlglot_policy(name="datafusion_compile")
expr = parse_sql("SELECT * FROM t WHERE id = :id", options=ParseSqlOptions(dialect="duckdb"))
compiled = compile_expr(expr, options=SqlGlotCompileOptions(policy=policy))
sql_text = sqlglot_emit(compiled, policy=policy)
```
