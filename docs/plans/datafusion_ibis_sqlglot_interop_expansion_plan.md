Below is a **comprehensive interop expansion plan** that is additive to
`docs/plans/PR1through4_integrated_implementation_plan.md`. It includes:

* all non-overlapping scope items from
  `docs/plans/PR1through5_integrated_expression_coverage_plan.md`, and
* the additional DataFusion/Ibis/SQLGlot interop items identified in the
  recent cross-reference review.

This plan **does not repeat PR-01..PR-04** work already in active
implementation. It focuses exclusively on **adapter coverage, safety,
rewrite tooling, and diagnostics**.

---

# PR Stack Overview (interop expansion)

**PR-05 — Expression Coverage + SQL Fallback (Ibis <-> SQLGlot <-> DataFusion)**

> Expand SQLGlot-to-DataFusion expression coverage, fix aggregate/join gaps,
> add a safe SQL fallback with parameter binding, and consolidate adapter
> entrypoints to prevent behavioral drift.

**PR-06 — Interop Hardening + Diagnostics (SQLGlot discipline + DDL + Substrait)**

> Add strict SQLGlot error handling, semantic diff artifacts, dialect tuning,
> AST rewrite hooks, DDL round-trips, SQL gating, nested data support, and
> Substrait artifacts for portability and regression testing.

---

# PR-05: Expression Coverage + SQL Fallback (Ibis <-> SQLGlot <-> DataFusion)

### Description (what changes)

This PR makes the SQLGlot-to-DataFusion adapter **complete and durable** by
expanding expression coverage, closing aggregate/join gaps, and adding a
safe SQL fallback with **typed parameter binding**. It also consolidates
entrypoints so all Ibis and SQLGlot execution paths share one policy surface.

---

## Scope item 5.1 — Expand scalar expression coverage

**Code pattern**

```python
def _add_expr(expr: exp.Add) -> Expr:
    return _expr_to_df(expr.left) + _expr_to_df(expr.right)

def _coalesce_expr(expr: exp.Coalesce) -> Expr:
    args = [_expr_to_df(arg) for arg in expr.expressions]
    return f.coalesce(*args)

def _case_expr(expr: exp.Case) -> Expr:
    result = _expr_to_df(expr.default) if expr.default is not None else lit(None)
    for when in reversed(expr.args.get("ifs") or []):
        result = f.when(_expr_to_df(when.this), _expr_to_df(when.args["true"]))
        result = result.otherwise(result)
    return result

_EXPR_DISPATCH.update(
    {
        exp.Add: cast("Callable[[Expression], Expr]", _add_expr),
        exp.Sub: cast("Callable[[Expression], Expr]", _sub_expr),
        exp.Mul: cast("Callable[[Expression], Expr]", _mul_expr),
        exp.Div: cast("Callable[[Expression], Expr]", _div_expr),
        exp.Neg: cast("Callable[[Expression], Expr]", _neg_expr),
        exp.Coalesce: cast("Callable[[Expression], Expr]", _coalesce_expr),
        exp.Case: cast("Callable[[Expression], Expr]", _case_expr),
        exp.Concat: cast("Callable[[Expression], Expr]", _concat_expr),
        exp.Like: cast("Callable[[Expression], Expr]", _like_expr),
        exp.ILike: cast("Callable[[Expression], Expr]", _ilike_expr),
        exp.In: cast("Callable[[Expression], Expr]", _in_expr),
        exp.Between: cast("Callable[[Expression], Expr]", _between_expr),
    }
)
```

**Target files**

- `src/datafusion_engine/df_builder.py`
- `src/sqlglot_tools/optimizer.py`

**Implementation checklist**

- [ ] Add handlers for arithmetic, case/coalesce, like/ilike, in/between, concat.
- [ ] Ensure new handlers align with SQLGlot node types.
- [ ] Add explicit casts when DataFusion requires typed literals.

---

## Scope item 5.2 — Aggregate + DISTINCT coverage

**Code pattern**

```python
def _aggregate_expr(expr: Expression) -> Expr:
    if isinstance(expr, exp.Count) and expr.this is None:
        return f.count(lit(1)).alias(expr.alias_or_name or "count")
    if isinstance(expr, exp.AggFunc) and expr.args.get("distinct"):
        arg = _expr_to_df(expr.expressions[0])
        return f.count_distinct(arg).alias(expr.alias_or_name or "count_distinct")
    if isinstance(expr, exp.AggFunc) and expr.args.get("filter"):
        base = _aggregate_expr(expr.copy().set("filter", None))
        predicate = _expr_to_df(expr.args["filter"].this)
        return base.filter(predicate)
    return _aggregate_expr_base(expr)

if select.args.get("distinct"):
    df = df.distinct()
```

**Target files**

- `src/datafusion_engine/df_builder.py`

**Implementation checklist**

- [ ] Add COUNT(*) support with explicit literal.
- [ ] Handle DISTINCT aggregates and SELECT DISTINCT.
- [ ] Implement aggregate FILTER clauses.

---

## Scope item 5.3 — Join key resolution + USING support

**Code pattern**

```python
def _join_keys(join: exp.Join) -> tuple[list[str], list[str]] | None:
    using = join.args.get("using")
    if using:
        keys = [col.name for col in using.expressions]
        return keys, keys
    return _join_keys_from_on(join.args.get("on"))

def _column_name(expr: Expression, *, strip_qualifier: bool = False) -> str | None:
    if isinstance(expr, exp.Column):
        if strip_qualifier:
            return expr.name
        return f"{expr.table}.{expr.name}" if expr.table else expr.name
    return None
```

**Target files**

- `src/datafusion_engine/df_builder.py`
- `src/sqlglot_tools/optimizer.py`

**Implementation checklist**

- [ ] Support JOIN ... USING in SQLGlot translation.
- [ ] Strip qualifiers when DataFusion expects unqualified join keys.
- [ ] Fail fast on ambiguous join keys.

---

## Scope item 5.4 — SQL fallback path for unsupported expressions

**Code pattern**

```python
def df_from_sqlglot_or_sql(
    ctx: SessionContext,
    expr: Expression,
    *,
    params: Mapping[str, object] | None = None,
) -> DataFrame:
    try:
        return df_from_sqlglot(ctx, expr)
    except TranslationError:
        sql = expr.sql(dialect="datafusion")
        bindings = datafusion_param_bindings(params or {})
        return ctx.sql(sql, param_values=bindings)
```

**Target files**

- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/df_builder.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**

- [ ] Add fallback helper that renders SQLGlot to DataFusion SQL.
- [ ] Use param_values for scalar bindings (no string interpolation).
- [ ] Emit diagnostics when fallback is used for coverage tracking.

---

## Scope item 5.5 — Parameter binding integration for Ibis execution

**Code pattern**

```python
def ibis_to_datafusion(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    params: Mapping[str, object] | None = None,
) -> DataFrame:
    sg_expr = ibis_to_sqlglot(expr, backend=backend)
    return df_from_sqlglot_or_sql(ctx, sg_expr, params=params)
```

**Target files**

- `src/ibis_engine/params_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**

- [ ] Thread param bindings through all DataFusion execution paths.
- [ ] Align Ibis param names with DataFusion param_values.
- [ ] Keep types intact for all bound values.

---

## Scope item 5.6 — Consolidate adapter entrypoints

**Code pattern**

```python
def compile_ibis_to_df(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    schema_map: SchemaMapping | None = None,
    optimize: bool = True,
    cache: bool = False,
    params: Mapping[str, object] | None = None,
) -> DataFrame:
    sg_expr = ibis_to_sqlglot(expr, backend=backend)
    if optimize:
        sg_expr = normalize_expr(sg_expr, schema=schema_map)
    df = df_from_sqlglot_or_sql(ctx, sg_expr, params=params)
    return df.cache() if cache else df
```

**Target files**

- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/datafusion_adapter.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**

- [ ] Route all Ibis -> DataFusion translation through one helper.
- [ ] Remove duplicate adapter logic to prevent drift.
- [ ] Keep optimization + caching behavior consistent across callers.

---

## Scope item 5.7 — Expression translation tests (coverage + fallback)

**Code pattern**

```python
def test_sqlglot_expr_addition(ctx: SessionContext, backend: BaseBackend) -> None:
    table = ibis.memtable({"a": [1], "b": [2]})
    expr = table.mutate(c=table.a + table.b)
    df = ibis_to_datafusion(expr, backend=backend, ctx=ctx)
    assert "c" in df.schema().names
```

**Target files**

- `tests/datafusion_engine/test_df_builder_expressions.py`
- `tests/datafusion_engine/test_df_builder_fallback.py`

**Implementation checklist**

- [ ] Add unit tests for new SQLGlot nodes.
- [ ] Add fallback-path tests for unsupported nodes.
- [ ] Skip tests cleanly if DataFusion is unavailable.

---

### Watchouts

* SQL fallback must preserve parameter types; no string interpolation.
* Join key qualification can silently mis-join; keep alias resolution strict.

---

# PR-06: Interop Hardening + Diagnostics (SQLGlot discipline + DDL + Substrait)

### Description (what changes)

This PR hardens interop by enforcing strict SQLGlot behavior, adding semantic
change diagnostics, and providing DDL and Substrait artifacts for portability
and regression testing. It also adds SQL gating for fallback execution and
extends translation to nested data access.

---

## Scope item 6.1 — Strict SQLGlot error policy (fail fast)

**Code pattern**

```python
from sqlglot import ErrorLevel

def render_sql(expr: Expression, *, dialect: str) -> str:
    return expr.sql(dialect=dialect, unsupported_level=ErrorLevel.RAISE)
```

**Target files**

- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**

- [ ] Add a strict render helper with unsupported_level=RAISE.
- [ ] Use strict rendering in diagnostics and fallback SQL generation.
- [ ] Surface parse/unsupported errors with actionable diagnostics.

---

## Scope item 6.2 — SQLGlot semantic diff artifacts

**Code pattern**

```python
from sqlglot import diff

def sqlglot_semantic_diff(left: Expression, right: Expression) -> list[object]:
    return diff(left, right)
```

**Target files**

- `src/sqlglot_tools/bridge.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**

- [ ] Add semantic diff helper between compiled/optimized ASTs.
- [ ] Emit diff artifacts in rule diagnostics bundles.
- [ ] Store diff summaries alongside SQL snapshots.

---

## Scope item 6.3 — DataFusion-tuned SQLGlot dialect overrides

**Code pattern**

```python
from sqlglot import Dialect, exp

class DataFusionDialect(Dialect):
    class Generator(Dialect.Generator):
        TRANSFORMS = {
            **Dialect.Generator.TRANSFORMS,
            exp.Cast: lambda self, e: f"CAST({self.sql(e.this)} AS {self.sql(e.to)})",
        }
```

**Target files**

- `src/sqlglot_tools/dialects.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**

- [ ] Add a DataFusion-specific dialect or generator overrides.
- [ ] Use the tuned dialect for SQL fallback rendering.
- [ ] Keep type mappings in sync with DataFusion expectations.

---

## Scope item 6.4 — AST rewrite hooks (policy injection)

**Code pattern**

```python
def rewrite_expr(expr: Expression) -> Expression:
    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Column) and node.table is None:
            return node.copy().set("table", exp.to_identifier("base"))
        return node
    return expr.transform(_rewrite)
```

**Target files**

- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**

- [ ] Add a rewrite pipeline hook before rendering or translation.
- [ ] Keep rewrites deterministic for stable SQL artifacts.
- [ ] Guard with feature flags to avoid unintentional changes.

---

## Scope item 6.5 — Schema round-trips to DataFusion DDL

**Code pattern**

```python
from sqlglot import exp

column_defs = schema.to_sqlglot_column_defs(dialect="datafusion")
stmt = exp.Create(
    this=exp.Table(this=exp.Identifier(this="events")),
    kind="TABLE",
    expression=exp.Schema(expressions=column_defs),
)
sql = stmt.sql(dialect="datafusion")
```

**Target files**

- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**

- [ ] Build DDL helpers from schema specs via SQLGlot ColumnDef.
- [ ] Support CREATE EXTERNAL TABLE with format options when needed.
- [ ] Use DDL helpers for reproducible IO contracts.

---

## Scope item 6.6 — SQL gating for fallback execution

**Code pattern**

```python
from datafusion import SQLOptions

opts = (
    SQLOptions()
    .with_allow_ddl(False)
    .with_allow_dml(False)
    .with_allow_statements(False)
)
return ctx.sql_with_options(sql, opts, param_values=bindings)
```

**Target files**

- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**

- [ ] Use SQLOptions to block DDL/DML in fallback SQL paths.
- [ ] Keep policy configurable for trusted vs untrusted callers.
- [ ] Log any policy violations as diagnostics.

---

## Scope item 6.7 — Nested data access translation

**Code pattern**

```python
def _bracket_expr(expr: exp.Bracket) -> Expr:
    base = _expr_to_df(expr.this)
    index = expr.expression
    if isinstance(index, exp.Literal) and not index.is_string:
        return base[int(index.this)]
    if isinstance(index, exp.Literal) and index.is_string:
        return base[index.this]
    raise TranslationError("Unsupported bracket access.")
```

**Target files**

- `src/datafusion_engine/df_builder.py`

**Implementation checklist**

- [ ] Map SQLGlot bracket access to DataFusion array/struct access.
- [ ] Support both integer index and string field access.
- [ ] Fail fast on unsupported nested access patterns.

---

## Scope item 6.8 — Substrait artifacts for portability

**Code pattern**

```python
from datafusion.substrait import Consumer, Serde

plan = df.logical_plan()
plan_bytes = Serde.serialize_bytes(plan)
replay = Consumer.from_substrait_plan(ctx, Serde.deserialize_bytes(plan_bytes))
```

**Target files**

- `src/datafusion_engine/bridge.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**

- [ ] Capture Substrait bytes as a diagnostics artifact.
- [ ] Provide replay helpers for regression testing.
- [ ] Gate Substrait usage behind feature flags if needed.

---

## Scope item 6.9 — SQLGlot executor test harness (smoke validation)

**Code pattern**

```python
from sqlglot import executor

def eval_sql_smoke(sql: str, *, tables: dict[str, object]) -> list[dict[str, object]]:
    return executor.execute(sql, tables=tables)
```

**Target files**

- `tests/sqlglot_tools/test_sqlglot_executor_smoke.py`

**Implementation checklist**

- [ ] Add smoke tests for SQLGlot AST rewrites.
- [ ] Keep fixtures small and deterministic.
- [ ] Use only for unit-level validation, not perf tests.

---

### Watchouts

* Custom dialect and rewrite hooks can change SQL rendering; keep changes gated.
* Substrait portability depends on function/type extension coverage.

---

# Global Implementation Notes

* This plan is additive to PR-01..PR-04 and does not overlap active work.
* Keep adapter changes strict and diagnostics-rich to preserve reproducibility.
* Treat SQL fallback as a safety valve, not the primary execution path.
