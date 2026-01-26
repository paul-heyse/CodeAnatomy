# View‑Driven CPG Pipeline — Best‑in‑Class Implementation Plan (v1)

## Purpose
Unify **extract → normalize → relspec → CPG** into a **view‑driven pipeline** where **all joins and derived datasets are created by view registration**, and **only two static declarations remain**:
1) extract base tables, and 2) final CPG output views. Everything else is **programmatically derived** (from view registry + spec registries) and validated by schema contracts.

## Guiding principles
- **View‑first execution**: all derived datasets are views created in DataFusion (not Python plan builders).
- **Contract‑first outputs**: every view has a schema contract; validation is information_schema‑driven.
- **AST‑first definitions**: views are defined as SQLGlot AST or DataFusion Expr builders for stability.
- **Rust‑UDF‑first semantics**: ID/hash/key logic comes from Rust UDFs in view expressions.
- **UDF snapshot‑gated views**: view registration requires the Rust UDF platform snapshot; missing UDFs fail fast.
- **No static join logic**: joins are declared via generated view specs; no manual plan builders.
- **Declarative orchestration**: registration is topologically sorted and dependency‑aware.

---

# Scope 0 — Rust UDF Platform Gate (view registration prerequisite)

## Goal
Guarantee that **Rust UDF installation and snapshot validation** occur **before any view registration**. Views must never register unless the UDF platform is present and validated.

Status: Complete.

### Representative pattern
```python
platform = install_rust_udf_platform(ctx)
validate_snapshot(platform.snapshot)
register_all_views(ctx, snapshot=platform.snapshot)
```

### Target files
- Modify: `src/datafusion_engine/udf_platform.py`
- Modify: `src/datafusion_engine/execution_facade.py`
- Modify: `src/datafusion_engine/view_graph_registry.py`

### Deletions
- Remove any ad‑hoc UDF registration calls inside view builders.

### Checklist
- [x] Enforce `install_rust_udf_platform(ctx)` before view registration.
- [x] Require snapshot validation (fail fast on missing docs/signatures).
- [x] Surface snapshot summary in diagnostics.

# Scope 1 — View Graph Registry (central orchestration)

## Goal
Introduce a **view graph registry** that declares every derived dataset as a view with explicit dependencies, schema, and builder. The registry is the **single orchestration surface** for normalize/relspec/cpg.

Status: Complete.

### Representative pattern
```python
@dataclass(frozen=True)
class ViewNode:
    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    schema_contract: SchemaContract
    required_udfs: tuple[str, ...] = ()
    sqlglot_ast: Expression | None = None


def register_view_graph(
    ctx: SessionContext,
    *,
    nodes: Sequence[ViewNode],
    snapshot: RustUdfSnapshot,
) -> None:
    validate_snapshot(snapshot)
    for node in topo_sort(nodes):
        validate_required_udfs(snapshot, node.required_udfs)
        df = node.builder(ctx)
        ctx.register_view(node.name, df)
```

### Target files
- New: `src/datafusion_engine/view_graph_registry.py`
- Modify: `src/datafusion_engine/view_registry.py` (delegate to graph registry)
- Modify: `src/datafusion_engine/schema_registry.py` (view specs/validation entrypoints)

### Deletions
- None (new capability only).

### Checklist
- [x] Define `ViewNode` structure with `name`, `deps`, `builder`, `schema_contract`.
- [x] Include `required_udfs` and enforce snapshot checks per view.
- [x] Implement topological registration with cycle detection.
- [x] Expose `register_all_views(ctx)` as the unified entrypoint.
- [x] Record registry diagnostics (UDF parity + fingerprints).

---

# Scope 1B — View Registry UDF Binding + Requirements Metadata

## Goal
Encode **UDF requirements directly into view schema metadata** and validate them against the Rust registry snapshot + information_schema during view registration.

Status: Partial (metadata enforcement done; hard-coded requirement lists still present).

### Representative pattern
```python
requirements = function_requirements_metadata_spec(
    required=("stable_id", "prefixed_hash64", "arrow_metadata"),
)
schema = requirements.apply(schema)
validate_required_functions(ctx, schema)
```

### Target files
- Modify: `src/datafusion_engine/view_graph_registry.py`
- Modify: `src/datafusion_engine/view_registry.py`
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/schema_contracts.py`
- Modify: `src/datafusion_engine/udf_runtime.py`

### Deletions
- Remove any Python‑side hard‑coded function requirement lists once metadata is authoritative.

### Checklist
- [x] Attach required UDF metadata to every view schema contract.
- [x] Validate required UDFs at view registration time.
- [x] Ensure information_schema.routines parity with snapshot metadata.
- [ ] Remove hard-coded required-UDF lists once metadata is authoritative.

---

# Scope 2 — Normalize Views (replace normalize plan builders)

## Goal
Replace `normalize/ibis_plan_builders.py` execution logic with **view builders** registered by the view graph. Normalize outputs become views derived directly from extract tables.

Status: Partial (views registered; legacy plan-builder API still used in normalize/ibis_api).

### Representative pattern
```python
# Example: normalize view derived from extract table

def view_scip_occurrences_norm(ctx: SessionContext) -> DataFrame:
    base = ctx.table("scip_occurrences_v1")
    return base.select(
        col("document_id"),
        col("symbol"),
        col("range_raw"),
        stable_id(lit("occ"), col("document_id"), col("symbol")).alias("occ_id"),
    )
```

### Target files
- Modify: `src/datafusion_engine/view_registry.py`
- New: `src/datafusion_engine/view_registry_specs.py` (normalize view specs)
- Modify: `src/normalize/view_builders.py` (view builder functions)
- Modify: `src/normalize/ibis_api.py` (remove plan-builder execution paths)

### Deletions
- Delete: `src/normalize/ibis_plan_builders.py` (renamed to view_builders)
- Delete: any normalize task catalog entries that only produced derived joins

### Checklist
- [x] Implement view specs for all normalize outputs currently built in Python.
- [x] Register normalize views via view graph registry.
- [ ] Remove normalize plan builder execution paths (normalize/ibis_api + ibis_bridge).
- [x] Add schema contract validation for normalize views.

---

# Scope 3 — Relspec Views (replace relationship plans)

## Goal
Express all relspec datasets as views over normalized inputs. `rel_*_v1` outputs and `relation_output_v1` must be view‑driven.

Status: Complete.

### Representative pattern
```python
# relation_output_v1 as view over rel_* views

def view_relation_output(ctx: SessionContext) -> DataFrame:
    rels = [ctx.table(name) for name in (
        "rel_name_symbol_v1",
        "rel_import_symbol_v1",
        "rel_def_symbol_v1",
        "rel_callsite_symbol_v1",
        "rel_callsite_qname_v1",
    )]
    return union_all(rels).select(
        col("src"), col("dst"), col("path"),
        col("bstart"), col("bend"),
        col("origin"), col("confidence"), col("score"),
        col("task_name"), col("task_priority"),
    )
```

### Target files
- Modify: `src/datafusion_engine/view_registry.py`
- New: `src/relspec/relationship_sql.py` (SQLGlot builders)
- Modify: `src/relspec/task_catalog.py` (remove relspec plan builders)

### Deletions
- Delete: `src/relspec/relationship_plans.py`
- Delete: any relspec task builders that emit rel_* datasets

### Checklist
- [x] Add relspec view definitions for each `rel_*_v1` output.
- [x] Define `relation_output_v1` view as union over relspec outputs.
- [x] Replace relspec plan building with view registry registration.

---

# Scope 4 — CPG Views (replace CPG plan builders)

## Goal
Make CPG outputs **pure views** (`cpg_nodes_v1`, `cpg_edges_v1`, `cpg_props_v1`). All edges/nodes/props must be computed in view SQL/Expr from relspec + normalize views.

Status: Complete.

### Representative pattern
```python
# cpg_edges_v1 from relation_output_v1

def view_cpg_edges(ctx: SessionContext) -> DataFrame:
    rel = ctx.table("relation_output_v1")
    return rel.select(
        stable_id(lit("edge"), col("src"), col("dst"), col("path"), col("bstart"), col("bend")).alias("edge_id"),
        col("kind").alias("edge_kind"),
        col("src").alias("src_node_id"),
        col("dst").alias("dst_node_id"),
        col("path"), col("bstart"), col("bend"),
        col("origin"), col("resolution_method"), col("confidence"), col("score"),
    )
```

### Target files
- Modify: `src/datafusion_engine/view_registry.py`
- New: `src/datafusion_engine/view_registry_specs.py` (CPG views from spec registry)
- New: `src/cpg/view_builders.py` (view builders)
- Modify: `src/cpg/spec_registry.py` (generate view mappings)

### Deletions
- Delete: `src/cpg/plan_builders.py`
- Delete: any CPG task builders that emit nodes/edges/props

### Checklist
- [x] Generate CPG node/edge/prop views from `cpg.spec_registry`.
- [x] Ensure Rust UDFs (`stable_id`, `prefixed_hash64`, etc.) are used in view expressions.
- [x] Register CPG view contracts in `schema_registry`.

---

# Scope 5 — Unified ViewSpec Generation (normalize/relspec/cpg)

## Goal
Auto‑generate view definitions from spec registries so **no join logic is manually hardcoded**. This is the “no static declarations” requirement.

Status: Partial (CPG + relspec are programmatic; normalize still enumerated manually).

### Representative pattern
```python
# generic builder from spec registry
for spec in node_plan_specs():
    views.append(ViewNode(
        name=f"cpg_nodes_{spec.name}",
        deps=(spec.table_ref,),
        builder=lambda ctx, spec=spec: build_node_view(ctx, spec),
        schema_contract=cpg_nodes_contract(),
    ))
```

### Target files
- New: `src/datafusion_engine/view_registry_specs.py`
- Modify: `src/cpg/spec_registry.py` (export spec registry data for views)
- Modify: `src/relspec/relationship_task_catalog.py` (emit view‑friendly spec data)

### Deletions
- None (new generator layer).

### Checklist
- [x] Provide helper to convert `NodePlanSpec` / `PropTableSpec` to view defs.
- [x] Provide helper to convert relspec specs to view defs.
- [x] Ensure stable view naming conventions for downstream use.
- [ ] Auto‑generate normalize view nodes from dataset specs/registry (remove manual enumeration).

---

# Scope 6 — View‑First Schema Contracts + Validation

## Goal
All views must be contract‑first and validated via `information_schema`, `SchemaContract`, and UDF requirement metadata.

Status: Partial (registration validates contracts; diagnostics for violations still pending).

### Representative pattern
```python
spec = view_spec_from_builder(ctx, name=view_name, builder=builder, sql=None)
contract = schema_contract_from_dataset_spec(name=view_name, spec=dataset_spec)
validate_schema_contract(contract, ctx)
validate_required_functions(ctx, contract.schema)
```

### Target files
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/view_registry.py`
- Modify: `src/datafusion_engine/schema_contracts.py`

### Deletions
- Remove any view schema inference helpers that bypass contracts.

### Checklist
- [x] Ensure every view registered has a SchemaContract.
- [x] Validate with `information_schema` after registration.
- [x] Validate UDF requirements at registration time.
- [ ] Record schema contract violations in diagnostics.
- [ ] Remove schema inference helpers that bypass contracts.

---

# Scope 7 — Rust UDF Enforcement in View Layer

## Goal
All ID/key/hash logic in views uses Rust UDFs, no Python helpers or fallback lanes.

Status: Partial (views use Rust UDFs, but Python hash wrappers remain).

### Representative pattern
```python
stable_id(lit("node"), col("file_id"), col("bstart"), col("bend"))
```

### Target files
- Modify: `src/datafusion_engine/view_registry_defs.py`
- Modify: `src/datafusion_engine/view_registry.py`
- Modify: `rust/datafusion_ext/src/udf_registry.rs`

### Deletions
- Delete remaining Python hash/ID helpers if still referenced.

### Checklist
- [x] Ensure all view expressions call Rust UDFs.
- [x] Verify UDF registry snapshot has all function requirements metadata.
- [ ] Reject any non‑Rust UDF usage in view builders (enforce/police).
- [ ] Remove any remaining Python‑side stable ID utilities (ibis_engine.hash_exprs, hashing helpers).

---

# Scope 8 — Materialization Strategy (post‑view)

## Goal
Materialization becomes optional and downstream of view creation. WritePipeline should consume views as its source of truth.

Status: Partial (view-based helpers added; orchestration still plan-centric).

### Representative pattern
```python
register_all_views(ctx)
result = facade.execute("SELECT * FROM cpg_edges_v1")
write_pipeline.write(result)
```

### Target files
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/datafusion_engine/execution_facade.py`
- Modify: `src/engine/materialize_pipeline.py`

### Deletions
- Remove any direct plan execution path for normalize/cpg outputs.

### Checklist
- [ ] Ensure materialization reads from view names in orchestration paths.
- [ ] Remove plan‑based materialization for normalize/cpg outputs.
- [x] Keep streaming executor integration intact.

---

# Scope 9 — SQLGlot AST Builders for Views

## Goal
Use SQLGlot AST generation for complex joins to ensure deterministic SQL and plan fingerprints.

Status: Partial (relspec SQLGlot views done; remaining joins still Ibis‑based).

### Representative pattern
```python
from sqlglot import select, exp
query = (
    select("src", "dst")
    .from_("relation_output_v1")
    .where(exp.column("confidence").gt(0.5))
)
ctx.sql(query.sql(dialect="datafusion"))
```

### Target files
- Modify: `src/datafusion_engine/view_registry.py`
- New: `src/sqlglot_tools/view_builders.py`

### Deletions
- None.

### Checklist
- [ ] Define SQLGlot builders for all multi‑join views (beyond relspec).
- [x] Emit deterministic SQL for view definitions.
- [x] Track view fingerprints for diagnostics.

---

# Scope 10 — View‑Driven UDF Diagnostics + Parity

## Goal
Emit diagnostics that prove **view → UDF dependency parity** across registry snapshot, information_schema, and view metadata.

Status: Complete.

### Representative pattern
```python
snapshot = rust_udf_snapshot(ctx)
parity = validate_view_udf_parity(ctx, snapshot, view_nodes)
record_artifact(profile, "view_udf_parity_v1", parity)
```

### Target files
- Modify: `src/datafusion_engine/diagnostics.py`
- Modify: `src/datafusion_engine/udf_runtime.py`
- Modify: `src/datafusion_engine/view_graph_registry.py`

### Deletions
- None (diagnostic surface only).

### Checklist
- [x] Emit per‑view UDF dependency coverage diagnostics.
- [x] Fail registration if required UDFs are missing or signatures mismatch.
- [x] Persist parity artifacts for build reproducibility.

# Deferred deletions (after all scope items complete)

These should **only be deleted after view graph fully replaces execution paths**:
- `src/normalize/task_catalog.py`
- `src/cpg/task_catalog.py`
- `src/relspec/task_catalog.py`
- Any “plan builder” or “execution” helpers for normalize/relspec/cpg that bypass views
- Any Python‑side UDF requirement maps once view metadata + snapshot parity is enforced

---

## Final state checklist
- [ ] All normalize/relspec/cpg datasets are created via view registration (normalize API still uses plan builders).
- [ ] All joins/derivations exist only in view builders (normalize/ibis_api remains plan‑based).
- [ ] Rust UDFs are used for all ID/key/hash operations (Python hash wrappers remain).
- [x] Schema contracts validated for every view.
- [x] View registration is snapshot‑gated with required‑UDF validation.
- [x] UDF parity diagnostics recorded for all views.
- [ ] Materialization consumes view outputs exclusively.
