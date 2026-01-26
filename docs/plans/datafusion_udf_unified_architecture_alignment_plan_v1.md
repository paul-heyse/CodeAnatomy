# DataFusion UDF Unified Architecture Alignment Plan (v1)

> **Goal**: Converge the codebase on a single, best‑in‑class execution architecture centered on Rust DataFusion UDFs + Ibis IR + SQLGlot AST, with view‑driven pipelines and Delta‑native IO. Design‑phase: aggressive migration and deletion is expected.

> **Principles**
> - **Single IR path**: Ibis → SQLGlot AST → canonicalized AST → execution (no SQL strings in internal paths).
> - **Single UDF source of truth**: Rust registry snapshot drives Ibis builtins, function registry, rewrite tags, docs.
> - **View‑first pipelines**: normalize/relspec/CPG are views, not ad‑hoc builders.
> - **Schema as contract**: Arrow/DF schema metadata + nested types are the contract surface.
> - **Delta‑native IO**: Delta TableProvider (including CDF) is the default scan path.

---

## Scope 1 — Canonical compilation pipeline (AST‑first execution)

**Goal**: Make SQLGlot AST the canonical compiled artifact. SQL strings are debug output only.  
**Why**: Eliminates SQL string divergence and makes policy, lineage, and cache keys stable.

### Representative pattern

```python
# src/datafusion_engine/compile_pipeline.py
compiled = compile_sql_policy(
    raw_ast,
    schema=schema_mapping,
    profile=policy_profile,
)
execution_plan = execute_sqlglot_ast(ctx, compiled.ast, dialect=policy_profile.write_dialect)
return CompiledExpression(
    ibis_expr=expr,
    sqlglot_ast=compiled.ast,
    rendered_sql=compiled.rendered_sql,  # debug only
    artifacts=compiled.artifacts,
)
```

### Target files
- `src/datafusion_engine/compile_pipeline.py`
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/sql_policy_engine.py`
- `src/sqlglot_tools/bridge.py`
- `src/sqlglot_tools/optimizer.py`

### Deletions
- Any internal use of `SessionContext.sql(...)` for pipeline‑generated SQL.

### Implementation checklist
- [ ] Add AST execution path (`execute_sqlglot_ast`) as default.
- [ ] Keep rendered SQL only as diagnostics artifact.
- [ ] Canonicalize: qualify → normalize identifiers → annotate types → canonicalize.
- [ ] Ensure lineage + fingerprint derived from AST, not SQL text.

---

## Scope 2 — Parameterization & prepared statements (no SQL interpolation)

**Goal**: Remove all SQL string interpolation in internal execution.  
**Why**: Guarantees type‑safe execution and injection‑safe control plane.

### Representative pattern

```python
# src/datafusion_engine/parameterized_execution.py
param = ibis.param("string")
expr = table.filter(table.col == param)
df = backend.execute(expr, params={"param": "value"})
```

### Target files
- `src/datafusion_engine/parameterized_execution.py`
- `src/datafusion_engine/sql_safety.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`

### Deletions
- SQL string substitution paths in parameterized execution.

### Implementation checklist
- [ ] Replace string substitution with `ibis.param` + `params`.
- [ ] Restrict SQL safety checks to external SQL ingress only.
- [ ] Remove any internal SQL string interpolation helpers.

---

## Scope 3 — Rust UDF platform as the only function registry

**Goal**: Ensure Rust snapshot drives all function metadata and binding.  
**Why**: Prevents drift and removes duplicate registries.

### Representative pattern

```python
# src/datafusion_engine/udf_platform.py
snapshot = register_rust_udfs(ctx, enable_async=...)
register_ibis_udf_snapshot(snapshot)
registry = build_function_registry(options=FunctionRegistryOptions(
    registry_snapshot=snapshot,
    datafusion_function_catalog=function_catalog,
))
```

### Target files
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_platform.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/engine/function_registry.py`
- `src/datafusion_engine/udf_catalog.py`

### Deletions
- Any remaining static UDF lists or manual operator maps used as authoritative metadata.

### Implementation checklist
- [ ] Snapshot includes arg names, return types, rewrite tags, docs.
- [ ] Ibis builtins are registered exclusively from snapshot.
- [ ] Function registry is snapshot‑only (info_schema validation optional).
- [ ] Remove any parallel UDF lane logic.

---

## Scope 4 — View‑driven pipeline unification (normalize/relspec/CPG)

**Goal**: Replace plan builders with view registry specs.  
**Why**: View specs become the only pipeline graph definition.

### Representative pattern

```python
# src/datafusion_engine/view_registry_specs.py
@view_spec("relation_output_v1")
def relation_output_v1(ctx: ViewBuildContext) -> IbisPlan:
    return cpg_edges_v1(ctx).union(cpg_nodes_v1(ctx))
```

### Target files
- `src/datafusion_engine/view_registry_specs.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_registry_defs.py`
- `src/normalize/*`
- `src/relspec/*`
- `src/cpg/*`

### Deletions
- Normalize/relspec/CPG legacy plan builders once views are authoritative:
  - `src/normalize/ibis_plan_builders.py`
  - `src/relspec/relationship_plans.py`
  - `src/cpg/plan_builders.py`

### Implementation checklist
- [ ] All pipelines expressed as views.
- [ ] View specs enforce UDF requirements + schema contracts.
- [ ] Materialization stage consumes views only.

---

## Scope 5 — Schema contracts: nested types as first‑class CPG ABI

**Goal**: Use DataFusion’s nested type functions as the canonical schema ABI.  
**Why**: Structured schema is the contract for CPG‑style data products.

### Representative pattern

```sql
SELECT
  named_struct('span', span_struct, 'attrs', attrs_map) AS node
FROM nodes_v1
```

### Target files
- `src/arrowdsl/schema/*`
- `src/datafusion_engine/schema_contracts.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/nested_tables.py`
- `src/datafusion_engine/udf_catalog.py`

### Deletions
- Legacy schema construction helpers that bypass Arrow schema metadata.

### Implementation checklist
- [ ] Standardize struct/map/union shapes for all view outputs.
- [ ] Preserve Arrow field metadata end‑to‑end.
- [ ] Use `map_*`, `union_*`, `get_field`, `unnest` idioms per DataFusion schema docs.

---

## Scope 6 — Delta‑native IO (TableProvider + CDF)

**Goal**: Prefer Delta TableProvider for all Delta scans; eliminate parquet fallbacks.  
**Why**: Delta offers stronger schema/metadata contracts and pruning.

### Representative pattern

```python
# src/datafusion_engine/registry_bridge.py
adapter.register_delta_table_provider(table_spec, artifacts)
adapter.register_delta_cdf_provider(table_spec, artifacts)
```

### Target files
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/*`
- `src/incremental/*`

### Deletions
- Parquet fallback paths for Delta in internal execution flows.

### Implementation checklist
- [ ] Enforce Delta TableProvider usage for Delta datasets.
- [ ] Use CDF provider for change capture.
- [ ] Expose DeltaScanConfig + object store wiring in IO adapter.

---

## Scope 7 — SQL ingress isolation

**Goal**: Limit SQL string surfaces to external ingress only.  
**Why**: Keeps internal execution AST‑first.

### Representative pattern

```python
# src/datafusion_engine/sql_safety.py
def validate_external_sql(sql: str, policy: SQLPolicyProfile) -> None:
    ast = parse_sql_strict(sql, dialect=policy.write_dialect)
    enforce_policy(ast, policy)
```

### Target files
- `src/datafusion_engine/sql_safety.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`

### Deletions
- Internal SQL execution helpers used in compiled pipelines.

### Implementation checklist
- [ ] Enforce SQL safety only for external SQL.
- [ ] Replace internal SQL execution with AST/IR execution.

---

## Scope 8 — Diagnostics & parity gates (UDF + schema)

**Goal**: Make UDF and schema parity checks mandatory at runtime.  
**Why**: Prevents drift between Rust registry, Ibis, and information_schema.

### Representative pattern

```python
report = udf_info_schema_parity_report(ctx)
if report.missing_in_information_schema:
    raise ValueError("UDF parity failed")
```

### Target files
- `src/datafusion_engine/udf_parity.py`
- `src/datafusion_engine/diagnostics.py`
- `src/relspec/evidence.py`

### Deletions
- None.

### Implementation checklist
- [ ] Enforce parity checks at runtime profile init.
- [ ] Record parity artifacts for diagnostics.

---

# Deferred decommissioning (only after scopes 1–8 are complete)

These items are legacy/parallel surfaces that can only be deleted once the new architecture is fully deployed and validated:
- `src/datafusion_engine/bridge.py` (legacy SQL compilation + execution helpers).
- `src/ibis_engine/expr_compiler.py` (manual operator maps once fully snapshot‑driven).
- `src/datafusion_engine/parameterized_execution.py` (if all execution moves to AST‑first + params).
- `src/ibis_engine/sql_bridge.py` (if external SQL ingestion is isolated elsewhere).
- `src/arrowdsl/schema/serialization.py` (if schema contracts fully move to DataFusion schema metadata).

---

## Final State Checklist
- [ ] No internal SQL string execution paths remain.
- [ ] Rust snapshot drives every UDF registration and metadata surface.
- [ ] View registry is the only pipeline definition surface.
- [ ] Delta TableProvider is the default scan path.
- [ ] Schema contracts enforced end‑to‑end with nested type ABI.
- [ ] Parity checks are enforced at runtime initialization.
