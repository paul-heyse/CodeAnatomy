# CPG Inference Streamline Plan

> **Goal**: fully align `src/cpg` with the inference‑first task/plan pipeline, remove legacy/unused spec surfaces, and streamline CPG plan construction using DataFusion/Delta/Ibis/SQLGlot capabilities already present in the codebase.

---

## 0) Target Architecture Snapshot

### Core CPG Build Flow (inference‑first)
```python
# cpg/task_catalog.py
TaskSpec(
    name="cpg.nodes",
    output="cpg_nodes_v1",
    build=_build_nodes,
    kind="compute",
    priority=100,
)
```

### Output Finalization (contract‑aligned)
```python
# hamilton_pipeline/modules/task_execution.py
table = task_outputs.outputs.get("cpg_nodes_v1")
final = normalize_only(table, contract=CPG_NODES_CONTRACT, ctx=ctx)
```

### Evidence‑safe plan inputs
```python
# cpg/plan_builders.py
plan = catalog.resolve_plan(name, ctx=ctx, label=name)
if plan is None:
    plan = register_ibis_table(empty_table(schema), options=SourceToIbisOptions(...))
```

---

## Scope 1 — Decommission Unused Legacy/Rule Surfaces

### Why
Remove dormant spec surfaces and constants that no longer participate in inference‑driven plan construction.

### Representative Code Snippet
```python
# cpg/specs.py
# Remove unused edge planning surfaces if not wired anywhere.
# EDGE_FILTERS, EdgePlanSpec, resolve_edge_filter
```

### Target Files
- `src/cpg/specs.py` (remove EdgePlanSpec, EDGE_FILTERS, resolve_edge_filter)
- `src/cpg/constants.py` (remove unused constants if not referenced)
- `src/cpg/scip_roles.py` (remove if only referenced by deleted constants)

### Implementation Checklist
- [x] Remove `EdgePlanSpec`, `EDGE_FILTERS`, and `resolve_edge_filter` from `src/cpg/specs.py`.
- [x] Delete unused constants in `src/cpg/constants.py` (`CpgBuildArtifacts`, `EDGE_ID_BASE`, `EDGE_ID_SPAN`, `edge_hash_specs`) if unused after cleanup.
- [x] Keep `src/cpg/scip_roles.py` because it remains referenced by `ROLE_FLAG_SPECS`.
- [x] Update `__all__` exports accordingly.

---

## Scope 2 — Simplify Spec Registry (Remove Option Flags)

### Why
Spec option flags are never consumed; they add complexity without controlling plan compilation. Replace with direct spec emission or TaskSpec‑level gating.

### Representative Code Snippet
```python
# cpg/spec_registry.py
@dataclass(frozen=True)
class EntityFamilySpec:
    name: str
    node_kind: NodeKindId
    id_cols: tuple[str, ...]
    node_table: str | None
    prop_source_map: Mapping[str, PropFieldInput] = field(default_factory=dict)
```

### Target Files
- `src/cpg/spec_registry.py`
- `src/cpg/specs.py`
- `src/cpg/plan_builders.py`

### Implementation Checklist
- [x] Remove `option_flag` fields from `NodePlanSpec` and `PropTableSpec`.
- [x] Remove `node_option_flag`/`prop_option_flag` from `EntityFamilySpec`.
- [x] Update `node_plan_specs()` and `prop_table_specs()` to ignore flags.
- [x] Ensure no downstream logic expects those flags.

---

## Scope 3 — Task Identity Metadata for Nodes/Props

### Why
Edges already carry `task_name`/`task_priority`. For consistent provenance, nodes/props should include the same task metadata (or emitted properties) when required.

### Representative Code Snippet
```python
# cpg/emit_nodes_ibis.py
output = output.mutate(
    task_name=ibis.literal(task_name),
    task_priority=ibis.literal(task_priority),
)
```

### Target Files
- `src/cpg/emit_nodes_ibis.py`
- `src/cpg/emit_props_ibis.py`
- `src/cpg/spec_registry.py` (optional prop fields for task metadata)

### Implementation Checklist
- [x] Register CPG schemas in `datafusion_engine.schema_registry` with task identity fields.
- [x] Thread task identity into builders and emission helpers for nodes/props.
- [x] Ensure output schemas remain validated.

---

## Scope 4 — Contract‑Aligned Finalization (DataFusion)

### Why
CPG outputs are aligned via `ensure_columns`, but contract enforcement and encoding should be handled through DataFusion finalize/normalize utilities to keep behavior consistent across the pipeline.

### Representative Code Snippet
```python
# cpg/emit_edges_ibis.py
normalized = normalize_only(table, contract=CPG_EDGES_CONTRACT, ctx=ctx)
```

### Target Files
- `src/cpg/emit_nodes_ibis.py`
- `src/cpg/emit_edges_ibis.py`
- `src/cpg/emit_props_ibis.py`
- `src/cpg/plan_builders.py` (if centralizing finalize step)

### Implementation Checklist
- [x] Introduce contract‑based normalization using `datafusion_engine.finalize.normalize_only`.
- [x] Finalize CPG outputs in the execution layer (`task_execution`) with contracts.
- [x] Keep ordering metadata behavior intact.

---

## Scope 5 — Streamline Prop Emission (Reduce Union Load)

### Why
`emit_props_ibis` currently unions many per‑prop rows, which can be heavy for SQL generation and compilation. Use batch union or SQLGlot‑assisted union generation.

### Representative Code Snippet
```python
# cpg/emit_props_ibis.py
rows = _batched_union(rows, batch_size=32)
```

### Target Files
- `src/cpg/emit_props_ibis.py`
- Optional: `src/sqlglot_tools/` (if generating SQLGlot unions directly)

### Implementation Checklist
- [x] Add batching for union of property rows.
- [x] Optionally emit SQLGlot union SQL for large prop specs.
- [x] Validate resulting schema matches `CPG_PROPS_SCHEMA`.

---

## Scope 6 — Catalog Preload and Missing Source Handling

### Why
Current fallback registers empty tables only when resolving inputs. Centralizing preloading improves lineage consistency and avoids silent missing inputs.

### Representative Code Snippet
```python
# cpg/plan_builders.py
preload_inputs(catalog, ctx=ctx, names=required_sources)
```

### Target Files
- `src/cpg/plan_builders.py`
- `src/datafusion_engine/schema_registry.py`

### Implementation Checklist
- [x] Add a pre‑compilation step that registers empty tables for missing sources.
- [x] Ensure missing source behavior is consistent and visible.
- [x] Maintain ordering metadata on empty tables.

---

## Decommission/Delete List (Summary)

### Files/Types to Remove
- `EdgePlanSpec`, `EDGE_FILTERS`, `resolve_edge_filter` from `src/cpg/specs.py`
- `CpgBuildArtifacts`, `EDGE_ID_BASE`, `EDGE_ID_SPAN`, `edge_hash_specs` from `src/cpg/constants.py` (if unused)
- `src/cpg/scip_roles.py` (retained; still referenced by `ROLE_FLAG_SPECS`)

### Optional Cleanups
- Remove `option_flag` fields from spec dataclasses and registry.

---

## Execution Order (Recommended)
1) Scope 1 (decommission/remove unused surfaces).
2) Scope 2 (spec registry simplification).
3) Scope 3 (task metadata alignment for nodes/props).
4) Scope 4 (contract‑aligned finalize/normalize).
5) Scope 5 (prop emission batching/SQLGlot union).
6) Scope 6 (catalog preload + missing source behavior).

---

## Completion Criteria
- No unused rule‑era spec surfaces remain in `src/cpg`.
- CPG outputs are normalized via DataFusion contract enforcement.
- Task provenance is consistent across nodes/edges/props.
- Property emission scales without excessive union overhead.
- Missing inputs handled via a consistent catalog preload policy.
