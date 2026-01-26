# DataFusion Unified Architecture — v2 Implementation Plan (Design-Phase, Breaking Changes OK)

> **Goal**: Advance beyond the v1 plan to a **fully unified, modular, view‑first architecture** driven by:  
> **Ibis IR → SQLGlot AST → canonical policy lane → DataFusion execution → Delta‑native IO**,  
> with a single UDF source of truth and a deterministic AST artifact registry.

> **Design-phase posture**: We optimize for **best‑in‑class target state**, even if it requires breaking changes or removal of legacy paths.

---

## Global design principles (non‑negotiable)

1. **AST-first, policy-driven** — SQLGlot AST is the canonical artifact; SQL text is debug‑only.  
2. **Single UDF source of truth** — Rust snapshot drives all UDF surfaces (Ibis, DataFusion registry, docs, parity).  
3. **View-first orchestration** — all pipelines are views; DAG derived programmatically.  
4. **Delta-native IO** — Delta TableProvider + CDF are the default read path; parquet fallbacks only for non‑Delta.  
5. **Schema-as-contract** — derived schema + metadata, with nested types as the ABI.  
6. **Deterministic artifacts** — serialized AST + policy hash drive cache keys, diffs, and invalidations.

---

# Scope 1 — Unified SQL policy lane (single canonical AST pipeline)

**Intent**: Merge `sql_policy_engine` and `sqlglot_tools/optimizer` into one **canonical policy engine** that always applies strict parsing, templating hygiene, dialect‑compat transforms, qualification, canonicalization, and deterministic AST serialization.

### Representative pattern

```python
# src/datafusion_engine/sql_policy.py (new consolidated entrypoint)
def compile_policy(expr: Expression, *, schema: SchemaMapping, profile: SQLPolicyProfile) -> PolicyResult:
    expr = sanitize_templated_sql(expr)                 # strip/render before parse
    expr = apply_transforms(expr, transforms=profile.transforms)
    expr = qualify(expr, schema=schema, expand_stars=profile.expand_stars)
    validate_qualify_columns(expr, sql=profile.original_sql)
    expr = annotate_types(expr, schema=schema)
    expr = canonicalize(expr, dialect=profile.read_dialect)
    expr = normalize_predicates(expr, max_distance=profile.normalize_distance_limit)
    artifacts = PolicyArtifacts.from_ast(expr, schema=schema)
    return PolicyResult(ast=expr, artifacts=artifacts)
```

### Target files to modify
- `src/datafusion_engine/sql_policy_engine.py`
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/sql_safety.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`

### Modules to delete
- Redundant policy path(s) once unified (remove the “secondary” canonicalization lane).
- Any internal SQL compilation helpers that bypass the policy engine.

### Implementation checklist
- [ ] Create a **single** canonical policy entrypoint used by all compile paths.  
- [ ] Centralize strict parsing + templated SQL sanitization.  
- [ ] Standardize transform lane (QUALIFY elimination, UNNEST normalization, join rewrites, move CTEs to top level).  
- [ ] Emit deterministic AST serde + policy hash; use as cache keys.  
- [ ] Remove policy duplication across `sql_policy_engine` and `sqlglot_tools/optimizer`.  

---

# Scope 2 — AST artifact registry (deterministic compilation artifacts)

**Intent**: Persist a stable AST artifact bundle at view registration time for **cache keys, invalidations, lineage, and diffs**.

### Representative pattern

```python
# src/datafusion_engine/view_artifacts.py (new)
@dataclass(frozen=True)
class ViewArtifact:
    name: str
    ast: Expression
    serde_payload: list[dict[str, object]]
    ast_fingerprint: str
    policy_hash: str
    schema: pa.Schema
    lineage: dict[str, set[tuple[str, str]]]
    required_udfs: tuple[str, ...]
```

### Target files to modify
- `src/datafusion_engine/view_graph_registry.py`
- `src/incremental/sqlglot_artifacts.py`
- `src/incremental/invalidations.py`
- `src/incremental/metadata.py`
- `src/datafusion_engine/semantic_diff.py`

### Modules to delete
- Any ad‑hoc AST fingerprinting paths once the artifact registry is canonical.

### Implementation checklist
- [ ] Create `ViewArtifact` and store it during view registration.  
- [ ] Update incremental invalidations to use artifact payloads.  
- [ ] Derive cache keys from `{ast_fingerprint + policy_hash}` only.  
- [ ] Expose artifact bundles as diagnostics payloads (consistent schema).  

---

# Scope 3 — Lineage + semantic diff hardening

**Intent**: Use SQLGlot scope caching and stable matchings for diffs; add a semantic equivalence test lane for rewrite safety.

### Representative pattern

```python
# src/sqlglot_tools/lineage.py
scope = build_scope(ast)             # cached by (serde_hash, schema_hash)
deps = referenced_tables(ast)
required = referenced_udf_calls(ast)
lineage = lineage(col, scope=scope, copy=False)
```

### Target files to modify
- `src/sqlglot_tools/lineage.py`
- `src/datafusion_engine/semantic_diff.py`
- `src/datafusion_engine/execution_helpers.py`
- `tests/*` (semantic rewrite correctness)

### Modules to delete
- None (harden, don’t remove).

### Implementation checklist
- [ ] Cache scope per AST serde hash + schema map hash.  
- [ ] Seed diff matchings to stabilize large AST diffs.  
- [ ] Add SQLGlot executor‑based equivalence tests for rewrites.  
- [ ] Harden diff classifications for window/joins/unnest cases.  

---

# Scope 4 — UDF platform v2 (registry + docs + SQL-defined wiring)

**Intent**: Rust UDF snapshot drives **all** UDF surfaces, including docs and named args; add `CREATE FUNCTION` factory and planner rewrites.

### Representative pattern

```python
# src/datafusion_engine/udf_platform.py
snapshot = register_rust_udfs(ctx, enable_async=True)
register_ibis_udf_snapshot(snapshot)
register_docs(snapshot)  # emits DataFusion Documentation + parameter names
```

### Target files to modify
- `src/datafusion_engine/udf_platform.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/function_factory.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/udf_parity.py`

### Modules to delete
- Static operator maps or manual UDF lists once snapshot‑driven.

### Implementation checklist
- [ ] Add parameter names + docs to the snapshot surface.  
- [ ] Emit DataFusion `Documentation` for SHOW FUNCTIONS parity.  
- [ ] Add `FunctionFactory` for SQL macro definitions (simplify rewrites).  
- [ ] Add `ExprPlanner` / `FunctionRewrite` for custom operators.  
- [ ] Remove parallel UDF metadata sources.  

---

# Scope 5 — Delta IO control plane (single Delta scan profile)

**Intent**: Consolidate Delta scan/log options and enforce Delta TableProvider + CDF provider usage across all read paths.

### Representative pattern

```python
# src/storage/deltalake/scan_profile.py (new)
def build_delta_scan_config(location: DatasetLocation) -> DeltaScanConfig:
    return DeltaScanConfig(
        file_column_name="__delta_rs_path",
        wrap_partition_values=True,
        enable_parquet_pushdown=True,
        schema_force_view_types=True,
    )
```

### Target files to modify
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/io_adapter.py`
- `src/ibis_engine/registry.py`
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/file_pruning.py`
- `src/incremental/*`

### Modules to delete
- Any parquet‑fallback path used for Delta tables once providers are enforced.

### Implementation checklist
- [ ] Centralize DeltaScanConfig + log storage option resolution.  
- [ ] Enforce Delta TableProvider for Delta datasets everywhere.  
- [ ] Use CDF provider for incremental when configured.  
- [ ] Standardize object store registration (once per runtime).  

---

# Scope 6 — Ibis as canonical view surface

**Intent**: View registration and IO must flow through Ibis to preserve IR‑first semantics and unify policy enforcement.

### Representative pattern

```python
# src/datafusion_engine/view_registry.py
backend = ibis.datafusion.connect(ctx)
expr = build_view_expr(...)
backend.create_view(name, expr, overwrite=True)
```

### Target files to modify
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/ibis_engine/registry.py`
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/execution_facade.py`

### Modules to delete
- Direct internal `ctx.sql` execution paths for view creation.

### Implementation checklist
- [ ] Use Ibis `create_view` for all view registrations.  
- [ ] Use Ibis `read_delta` / `to_delta` for Delta IO surfaces.  
- [ ] Ensure view DAG registration uses Ibis IR and captures AST artifacts.  

---

# Scope 7 — Schema ABI enforcement (nested types as contract)

**Intent**: Make nested struct/map/union shapes the canonical ABI for all view outputs; enforce via schema contracts.

### Representative pattern

```python
# src/datafusion_engine/schema_contracts.py
contract = SchemaContract.from_arrow_schema(name, schema)
contract = contract.apply_metadata(required_udfs, ordering_keys, evidence_meta)
validate_schema_contract(contract, ctx)
```

### Target files to modify
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/schema_contracts.py`
- `src/arrowdsl/schema/*`
- `src/datafusion_engine/nested_tables.py`

### Modules to delete
- Legacy schema helpers that bypass Arrow metadata or produce ad‑hoc layouts.

### Implementation checklist
- [ ] Standardize nested ABI shapes (map/struct/union) for all views.  
- [ ] Remove static view schema registries (view schema derived).  
- [ ] Enforce ABI in schema contracts during view registration.  

---

# Scope 8 — Programmatic scheduling across views + datasets

**Intent**: Use rustworkx DAG across **views + datasets** with deterministic ordering and incremental rebuild logic.

### Representative pattern

```python
# src/relspec/graph_inference.py
graph = build_graph(view_nodes, dataset_nodes)
order = rx.lexicographical_topological_sort(graph, key=lambda n: n.name)
```

### Target files to modify
- `src/relspec/graph_inference.py`
- `src/relspec/incremental.py`
- `src/incremental/invalidations.py`

### Modules to delete
- Any remaining static task catalogs or manual ordering logic.

### Implementation checklist
- [ ] Build DAG from view specs + dataset specs.  
- [ ] Deterministic topo‑sort with lexicographic tie‑breaks.  
- [ ] Incremental rebuild via ancestor/descendant sets.  

---

# Scope 9 — SQL ingress isolation + templating hygiene

**Intent**: External SQL ingress is allowed; internal SQL must remain AST‑first. Add templating sanitization before parse.

### Representative pattern

```python
# src/datafusion_engine/sql_safety.py
def sanitize_external_sql(sql: str) -> str:
    if contains_templating(sql):
        return render_or_replace(sql)
    return sql
```

### Target files to modify
- `src/datafusion_engine/sql_safety.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/compile_pipeline.py`

### Modules to delete
- Internal SQL execution helpers not strictly for external ingress.

### Implementation checklist
- [ ] Templated SQL preprocessing before parse.  
- [ ] External SQL only; internal paths compile AST.  
- [ ] Eliminate remaining internal SQL builders (DESCRIBE/SHOW).  

---

# Scope 10 — Diagnostics + parity gates (AST/UDF/schema)

**Intent**: Diagnostics become deterministic and policy‑aware; parity gates are enforced at runtime initialization.

### Representative pattern

```python
# src/datafusion_engine/udf_parity.py
report = udf_info_schema_parity_report(ctx, snapshot)
if report.missing_in_information_schema:
    raise ValueError("UDF parity failed")
```

### Target files to modify
- `src/datafusion_engine/udf_parity.py`
- `src/datafusion_engine/diagnostics.py`
- `src/datafusion_engine/schema_registry.py`
- `src/incremental/metadata.py`

### Modules to delete
- None (harden, don’t remove).

### Implementation checklist
- [ ] Ensure diagnostics payloads include AST policy hash + serde fingerprint.  
- [ ] Enforce parity at runtime init (UDF + schema).  
- [ ] Standardize diagnostics schemas for plan artifacts.  

---

# Deferred decommissioning (only after all scopes complete)

These cannot be safely removed until the unified architecture is fully deployed and validated:

### Deferred deletion list
- `src/ibis_engine/expr_compiler.py` (manual operator maps)  
- `src/datafusion_engine/parameterized_execution.py` (if all execution is AST‑first + params)  
- `src/ibis_engine/sql_bridge.py` (if external SQL ingress is fully isolated elsewhere)  
- `src/arrowdsl/schema/serialization.py` (if schema contracts fully move to metadata)  
- Any remaining static view schema registries or column maps  
- Any remaining plan builder modules still referenced by orchestration

### Checklist
- [ ] All view registration flows through Ibis.  
- [ ] All internal SQL execution removed (AST‑only).  
- [ ] Delta TableProvider + CDF enforced everywhere.  
- [ ] Schema ABI enforcement complete (no static view schema registries remain).  
- [ ] UDF snapshot parity checks pass with zero drift.  

---

## Final State Acceptance Criteria

- [ ] **AST is the single canonical artifact**; SQL text is debug only.  
- [ ] **One policy lane** (no duplicated canonicalization or transform logic).  
- [ ] **Rust UDF snapshot** drives Ibis + DataFusion + docs + parity.  
- [ ] **All pipelines are view‑defined** and DAG‑scheduled programmatically.  
- [ ] **Delta TableProvider + CDF** is the default for Delta IO.  
- [ ] **Schema contracts** validate nested ABI shapes end‑to‑end.  
- [ ] **Deterministic artifacts** (serde payload + policy hash) power cache/diff.  

