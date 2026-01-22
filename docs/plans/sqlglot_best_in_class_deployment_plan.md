# SQLGlot Best-in-Class Deployment Plan

## Scope

This plan upgrades SQLGlot usage to a best-in-class, production-grade policy engine. The scope includes:

- Pivot canonicalization to `optimize()` with a pinned rule list.
- Support parameterized SQL (placeholders preserved + safe binding).
- Adopt a canonical `duckdb` dialect for IR, while emitting DataFusion-specific SQL at execution boundaries.
- Expand the existing DataFusion generator with a uniform transform chain (no new custom dialects).
- Standardize parsing/diff entrypoints and eliminate ad-hoc `parse_one` usage.
- Unify AST and schema hashing, and eliminate duplicate fingerprint logic.
- Ensure generator emission uses policy-accurate quoting/unsupported behavior.
- Add tests and diagnostics that lock semantic correctness of rewrites.

---

## Status (Updated)

- Implementation completed across core SQLGlot policy, dialect, transforms, and parameter handling.
- Documentation, emission, and regression test follow-ups are complete.

---

## Item 1: Canonical Pipeline on `optimize()` (Policy Alignment)

### Goal
Make `optimize()` the canonical normalization path and ensure policy snapshots reflect the actual rewrite pipeline.

### Design / Architecture
Introduce a single compile function that applies transforms, runs `optimize()`, and returns both the optimized expression and stats. Keep `normalize_expr` as a thin wrapper so call sites can be migrated incrementally.

```python
# src/sqlglot_tools/optimizer.py
@dataclass(frozen=True)
class SqlGlotCompileOptions:
    schema: SchemaMapping | None = None
    rewrite_hook: Callable[[Expression], Expression] | None = None
    enable_rewrites: bool = True
    policy: SqlGlotPolicy | None = None
    sql: str | None = None


def compile_expr(
    expr: Expression,
    *,
    options: SqlGlotCompileOptions,
) -> Expression:
    policy = options.policy or default_sqlglot_policy()
    canonical = canonicalize_expr(expr, rules=None)
    rewritten = rewrite_expr(
        canonical,
        rewrite_hook=options.rewrite_hook,
        enabled=options.enable_rewrites,
    )
    transformed = apply_transforms(rewritten, transforms=policy.transforms)
    return optimize_expr(transformed, schema=options.schema, policy=policy)
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/query_fragments.py`
- `src/normalize/runner.py`

### Implementation Checklist
- [x] Add `SqlGlotCompileOptions` + `compile_expr`.
- [x] Make `normalize_expr` delegate to `compile_expr` or explicitly migrate call sites.
- [x] Ensure `SqlGlotPolicySnapshot.rules` reflects the actual optimizer rules in use.
- [x] Add a migration note in docs for any deprecated normalization path.

---

## Item 2: Dialect Strategy (DuckDB Canonical IR + DataFusion Emission)

### Goal
Use DuckDB as the canonical IR dialect while emitting DataFusion SQL at execution boundaries via the existing `datafusion_ext` generator.

### Design / Architecture
Pin the canonical policy dialect to DuckDB, and override dialects only at execution/DDL surfaces. Keep the DataFusion generator minimal but strict.

```python
# src/sqlglot_tools/optimizer.py
DEFAULT_CANONICAL_DIALECT: str = "duckdb"
DEFAULT_WRITE_DIALECT: str = "duckdb"

DEFAULT_SURFACE_POLICIES: dict[SqlGlotSurface, SqlGlotSurfacePolicy] = {
    SqlGlotSurface.DATAFUSION_COMPILE: SqlGlotSurfacePolicy(
        dialect="datafusion_ext",
        lane="dialect_shim",
    ),
    # ...
}
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/compile_options.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/view_specs.py`

### Implementation Checklist
- [x] Introduce canonical dialect constants and update `default_sqlglot_policy`.
- [x] Ensure DataFusion execution uses `datafusion_ext` at the surface policy layer.
- [x] Verify any DuckDB-specific functions/types used in IR are valid.

---

## Item 3: DataFusion Generator Transform Chain (Best-in-Class Emission)

### Goal
Ensure DataFusion emission always applies the same compatibility transforms and DDL/DML formatting rules.

### Design / Architecture
Use `sqlglot.transforms.preprocess` to register a uniform transform chain at generation time.

```python
# src/sqlglot_tools/optimizer.py
from sqlglot.transforms import preprocess

DATAFUSION_TRANSFORMS = preprocess(
    [
        eliminate_qualify,
        move_ctes_to_top_level,
        eliminate_semi_and_anti_joins,
        _rewrite_full_outer_join,
        ensure_bools,
    ]
)

class DataFusionGenerator(SqlGlotGenerator):
    TRANSFORMS = {
        **SqlGlotGenerator.TRANSFORMS,
        exp.Select: DATAFUSION_TRANSFORMS,
    }
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/view_specs.py`

### Implementation Checklist
- [x] Add a generator-level transform chain via `preprocess`.
- [x] Keep DDL-specific property transforms in the generator.
- [x] Add regression tests for `CREATE EXTERNAL TABLE` and `QUALIFY`.

---

## Item 4: Parameterized SQL Support (Preserve Placeholders)

### Goal
Support parameterized SQL (`:name`, `$1`, `?`) without losing markers during parsing.

### Design / Architecture
Introduce a parse options object and a parameter-preserving parse helper. Use it for DML/exec paths and preflight when parameters are present.

```python
# src/sqlglot_tools/optimizer.py
@dataclass(frozen=True)
class ParseSqlOptions:
    dialect: str
    error_level: ErrorLevel | None = None
    sanitize_templated: bool = True
    preserve_params: bool = False


def parse_sql(
    sql: str,
    *,
    options: ParseSqlOptions,
) -> Expression:
    sanitized = sql
    if options.sanitize_templated:
        sanitized = sanitize_templated_sql(sql, preserve_params=options.preserve_params)
    level = options.error_level or DEFAULT_ERROR_LEVEL
    return parse_one(sanitized, dialect=options.dialect, error_level=level)
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/runtime.py`

### Implementation Checklist
- [x] Add `ParseSqlOptions` + `parse_sql`.
- [x] Update `sanitize_templated_sql` to preserve parameter markers when requested.
- [x] Use `parse_sql(..., preserve_params=True)` for DML/exec/preflight when params exist.
- [x] Expand `bind_params` to accept numeric placeholders (`$1`) and normalize names.
- [x] Add tests for parameterized SQL parsing + binding.

---

## Item 5: Parsing and Diff Entry Points (Single Policy Surface)

### Goal
Eliminate ad-hoc `parse_one`/`sqlglot.diff` usage and use the unified parsing/diff helpers.

### Design / Architecture
Provide wrapper helpers for parsing and diffing with consistent options.

```python
# src/sqlglot_tools/compat.py
def semantic_diff(left: Expression, right: Expression) -> Iterable[object]:
    return diff(left, right)


# src/obs/manifest.py (example change)
expr = parse_sql(
    optimized_sql,
    options=ParseSqlOptions(dialect=str(dialect), sanitize_templated=True),
)
```

### Target Files
- `src/obs/repro.py`
- `src/obs/manifest.py`
- `src/ibis_engine/plan_diff.py`
- `src/sqlglot_tools/compat.py`

### Implementation Checklist
- [x] Replace direct `parse_one` usage with `parse_sql`.
- [x] Use `sqlglot_tools.compat.diff` consistently for semantic diffs.
- [x] Add a test fixture for parse + diff stability.

---

## Item 6: Unified Hashing and Fingerprints

### Goal
Eliminate duplicate AST fingerprint logic and ensure schema hashes are computed consistently.

### Design / Architecture
Use a single canonical AST fingerprint helper and a shared schema map hash.

```python
# src/sqlglot_tools/lineage.py
from sqlglot_tools.optimizer import canonical_ast_fingerprint

# src/sqlglot_tools/optimizer.py (preflight)
schema_map_hash = (
    schema_map_fingerprint_from_mapping(schema) if schema is not None else None
)
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/lineage.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/relspec/execution_bundle.py`

### Implementation Checklist
- [x] Remove duplicate `canonical_ast_fingerprint` in `lineage.py`.
- [x] Use `schema_map_fingerprint_from_mapping` for preflight hashing.
- [x] Ensure plan signatures include policy hash + rules hash + schema hash.

---

## Item 7: Emission Helper and Policy-Accurate Quoting

### Goal
Ensure SQL emission honors `identify`, `unsupported_level`, and generator options from policy.

### Design / Architecture
Add a centralized SQL emission helper and make `sqlglot_sql` use policy quoting.

```python
# src/sqlglot_tools/optimizer.py
def sqlglot_emit(
    expr: Expression,
    *,
    policy: SqlGlotPolicy,
    pretty: bool | None = None,
) -> str:
    generator = dict(_generator_kwargs(policy))
    if pretty is not None:
        generator["pretty"] = pretty
    generator["dialect"] = policy.write_dialect
    generator["identify"] = policy.identify
    generator["unsupported_level"] = policy.unsupported_level
    return expr.sql(**cast("GeneratorInitKwargs", generator))
```

### Target Files
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/sql_bridge.py`
- `src/schema_spec/specs.py`

### Implementation Checklist
- [x] Implement `sqlglot_emit` and update `sqlglot_sql` or call sites.
- [x] Ensure all SQL emission uses the helper.
- [x] Add a quoting regression test for uppercase identifiers.

---

## Item 8: AST Execution Path (Keep the No-String Lane Healthy)

### Goal
Keep the SQLGlot AST execution path viable and covered by tests.

### Design / Architecture
Add a compile option to choose AST execution and test it with a minimal DataFusion backend.

```python
# src/datafusion_engine/compile_options.py
@dataclass(frozen=True)
class DataFusionCompileOptions:
    # ...
    prefer_ast_execution: bool = False


# src/datafusion_engine/bridge.py
if resolved.prefer_ast_execution:
    return df_from_sqlglot_ast(ctx, rewritten, options=resolved)
```

### Target Files
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/bridge.py`
- `tests/sqlglot/test_rewrite_regressions.py`

### Implementation Checklist
- [x] Add `prefer_ast_execution` to compile options.
- [x] Route to `df_from_sqlglot_ast` when enabled.
- [x] Add a regression test that uses AST execution and validates results.

---

## Item 9: Policy Helper Consolidation

### Goal
Remove local policy builders and use `resolve_sqlglot_policy` consistently.

### Design / Architecture
Replace per-module policy creation with the centralized resolver.

```python
# src/ibis_engine/sql_bridge.py
policy = resolve_sqlglot_policy(
    name="datafusion_compile",
    policy=None,
)
policy = replace(policy, identify=True, validate_qualify_columns=True)
```

### Target Files
- `src/ibis_engine/sql_bridge.py`
- `src/ibis_engine/plan_diff.py`
- `src/schema_spec/specs.py`
- `src/normalize/runner.py`

### Implementation Checklist
- [x] Replace local policy helpers with `resolve_sqlglot_policy`.
- [x] Keep override adjustments (`identify`, `validate_qualify_columns`) explicit.
- [x] Update docstrings for any policy surfaces.

---

## Item 10: Tests and Diagnostics

### Goal
Lock semantic equivalence of rewrites and provide reproducible diagnostics.

### Design / Architecture
Expand SQLGlot executor tests and add test fixtures for optimize() outputs, parameterized SQL, and generator transforms.

```python
# tests/sqlglot/test_rewrite_regressions.py
def test_optimize_pipeline_semantics() -> None:
    sql = "SELECT a FROM t WHERE b > 0"
    policy = default_sqlglot_policy()
    expr = parse_sql(sql, options=ParseSqlOptions(dialect=policy.read_dialect))
    optimized = optimize_expr(expr, policy=policy)
    assert _execute_expr(optimized, tables={"t": [{"a": 1, "b": 1}]}) == ((1,),)
```

### Target Files
- `tests/sqlglot/test_rewrite_regressions.py`
- `tests/integration/test_substrait_cross_validation.py`
- `scripts/sqlglot_semantic_harness.py`

### Implementation Checklist
- [x] Add optimize() semantic regression tests.
- [x] Add parameterized SQL parse/bind tests.
- [x] Add generator transform tests for DataFusion DDL.

---

## Global Implementation Checklist

- [x] Introduce `compile_expr` with `optimize()` as the canonical pipeline.
- [x] Move to a DuckDB canonical dialect and DataFusion emission surfaces.
- [x] Add parameterized SQL parsing and binding support.
- [x] Standardize parse/diff entrypoints.
- [x] Unify AST + schema hashing.
- [x] Enforce policy-accurate SQL emission (`identify`, `unsupported_level`).
- [x] Keep AST execution path tested.
- [x] Expand SQLGlot regression tests for rewrites and parameters.

---
