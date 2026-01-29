# Hamilton + rustworkx Capability Expansion Best‑in‑Class Plan (Design‑Phase)

Date: 2026-01-29  
Owner: Codex (design phase)  
Status: draft (proposed)  
Scope: Hamilton advanced capabilities + rustworkx advanced graph analytics

## Purpose
Adopt the highest‑leverage Hamilton and rustworkx capabilities that are currently unused or under‑utilized, and refactor the orchestration layer to fully exploit them. This plan targets **best‑in‑class** functionality, accepts breaking changes, and prioritizes observability, reproducibility, and performance.

## Design Principles
- **Observability is first‑class**: UI‑level lineage, run telemetry, artifact catalog, and cache audit logs are mandatory.
- **Determinism is explicit**: every scheduling decision and cache hit is reproducible from structured metadata.
- **Graph intelligence wins**: rustworkx analytics (dominators, centrality, cuts) inform scheduling, pruning, and admission.
- **Config shapes the DAG**: runtime configuration can reshape wiring without branching inside node bodies.
- **Breaking changes are acceptable** in favor of a coherent target architecture.

---

## Scope 1 — Hamilton UI/Telemetry Tracking (full‑fidelity run observability)

### Objective
Integrate Hamilton’s tracking adapters to publish DAG versions, run telemetry, node timings, and artifact metadata into the Hamilton UI (self‑hosted or DAGWorks hosted). This becomes the canonical observability layer for plan execution.

### Representative code patterns
```python
# src/hamilton_pipeline/driver_factory.py
from hamilton_sdk import adapters as h_tracking

tracker = h_tracking.HamiltonTracker(
    project_id=project_id,
    username=runtime_user,
    dag_name=dag_name,
    tags={
        "environment": runtime_env,
        "team": "codeintel",
        "version": plan.plan_signature,
    },
    hamilton_api_url=api_url,
    hamilton_ui_url=ui_url,
)

builder = builder.with_adapters(tracker)
```

### Target files to modify
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/engine/runtime_profile.py`
- `docs/architecture/part_vi_cpg_build_and_orchestration.md`

### Modules to delete
- None.

### Implementation checklist
- [ ] Add tracker wiring to Driver builder with explicit config surface.
- [ ] Add environment/identity tags (team, tier, version, profile).
- [ ] Ensure tracker endpoints are configurable via runtime profile.
- [ ] Update documentation for UI setup and telemetry governance.

---

## Scope 2 — Hamilton Cache Stores + Structured Cache Logs

### Objective
Upgrade caching to best‑in‑class: separate metadata/result stores, JSONL log streams, explicit caching policy, and reproducible cache audit trails.

### Representative code patterns
```python
# src/hamilton_pipeline/driver_factory.py
from hamilton.caching.stores.sqlite import SQLiteMetadataStore
from hamilton.caching.stores.file import FileResultStore

metadata_store = SQLiteMetadataStore(path=str(cache_root / "metadata.sqlite"))
result_store = FileResultStore(path=str(cache_root / "results"))

builder = builder.with_cache(
    metadata_store=metadata_store,
    result_store=result_store,
    default_behavior="disable",
    default=["execution_plan", "plan_artifacts", "write_*"],
    log_to_file=True,
)
```

### Target files to modify
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/execution_plan.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `docs/architecture/ARCHITECTURE.md`

### Modules to delete
- None.

### Implementation checklist
- [ ] Add cache store configuration to runtime profile/config surface.
- [ ] Enable JSONL cache logs and expose location in run manifest.
- [ ] Define explicit cache behaviors (IGNORE/RECOMPUTE/DISABLE) for inputs + external I/O.
- [ ] Document cache governance + invalidation patterns.

---

## Scope 3 — Config‑Driven DAG Rewiring (resolve/inject)

### Objective
Replace config‑gated branching in node bodies with compile‑time DAG rewiring using Hamilton’s `@resolve` / `@resolve_from_config` and `@inject`.

### Representative code patterns
```python
# src/hamilton_pipeline/modules/inputs.py
from hamilton.function_modifiers import resolve, ResolveAt, inject, source

@resolve(
    when=ResolveAt.CONFIG_AVAILABLE,
    decorate_with=lambda cfg: inject(
        feature_registry=source(cfg["feature_registry_node"])
    ),
)
def feature_inputs(feature_registry: dict[str, object]) -> dict[str, object]:
    return feature_registry
```

### Target files to modify
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/task_execution.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/driver_factory.py`

### Modules to delete
- Config‑branching helper utilities embedded in node bodies (to be removed case‑by‑case).

### Implementation checklist
- [ ] Identify config‑gated branches in task execution and outputs.
- [ ] Replace with resolve/inject rewiring on build.
- [ ] Ensure plan artifacts capture resolved DAG shape and config.

---

## Scope 4 — Parameterized Sub‑DAGs for Repeated Execution Families

### Objective
Use `@parameterized_subdag` and parameterization modifiers to generate repeated DAG slices (per language, per dataset, per artifact family), replacing duplicated manual module construction.

### Representative code patterns
```python
# src/hamilton_pipeline/modules/feature_families.py
from hamilton.function_modifiers import parameterized_subdag, value

@parameterized_subdag(
    load_from=[feature_module],
    python={"inputs": {"lang": value("py")}},
    rust={"inputs": {"lang": value("rs")}},
    typescript={"inputs": {"lang": value("ts")}},
)
def language_features(feature_df: object) -> object:
    return feature_df
```

### Target files to modify
- `src/hamilton_pipeline/modules/*`
- `src/hamilton_pipeline/task_module_builder.py`
- `src/hamilton_pipeline/driver_factory.py`

### Modules to delete
- Duplicated per‑language/per‑artifact module builders.

### Implementation checklist
- [ ] Identify duplicated DAG assembly patterns.
- [ ] Replace with parameterized sub‑DAGs.
- [ ] Update tests and plan signature derivation for parameterized modules.

---

## Scope 5 — Column‑Level Sub‑DAGs (with_columns / extract_columns)

### Objective
Introduce column‑subDAGs for dense tabular transforms to improve lineage, memory, and observability.

### Representative code patterns
```python
# src/normalize/df_view_builders.py
from hamilton.plugins.h_polars import with_columns

@with_columns(
    normalize_paths,
    normalize_language,
    columns_to_pass=["path", "language"],
    select=["path_norm", "language_norm"],
    namespace="norm",
)
def normalized_files(df: object) -> object:
    return df
```

### Target files to modify
- `src/normalize/*`
- `src/cpg/*`
- `src/hamilton_pipeline/modules/outputs.py`

### Modules to delete
- Redundant normalization helper nodes that only pipe columns through.

### Implementation checklist
- [ ] Identify dense column transforms suitable for column‑subDAGs.
- [ ] Replace with `with_columns` or `extract_columns` patterns.
- [ ] Tag and namespace column nodes for UI + diagnostics.

---

## Scope 6 — Enhanced Validation Surface (check_output + schema tags)

### Objective
Use `@check_output` / `@check_output_custom` / schema tags for stronger runtime validation on critical outputs and intermediate tables.

### Representative code patterns
```python
# src/hamilton_pipeline/modules/outputs.py
from hamilton.function_modifiers import check_output

@check_output(
    data_type=dict,
    importance="fail",
)
def runtime_profile_snapshot(...) -> dict:
    ...
```

### Target files to modify
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/modules/task_execution.py`
- `src/normalize/*`

### Modules to delete
- None.

### Implementation checklist
- [ ] Add check_output on high‑value artifacts.
- [ ] Adopt schema tags for DataFrame outputs where meaningful.
- [ ] Integrate validation failures into plan artifacts + diagnostics.

---

## Scope 7 — Advanced Graph Analytics for Scheduling (rustworkx)

### Objective
Leverage rustworkx analytics (dominators, centrality, bridges/cuts) to drive improved scheduling, pruning, and admission control.

### Representative code patterns
```python
# src/relspec/rustworkx_graph.py
import rustworkx as rx

critical_dominators = rx.immediate_dominators(graph, start_node)
bridge_edges = rx.bridges(undirected_view)
centrality = rx.betweenness_centrality(graph)
```

### Target files to modify
- `src/relspec/rustworkx_graph.py`
- `src/relspec/execution_plan.py`
- `src/relspec/rustworkx_schedule.py`
- `src/hamilton_pipeline/scheduling_hooks.py`

### Modules to delete
- None.

### Implementation checklist
- [ ] Compute dominator sets for critical scheduling gates.
- [ ] Add centrality metrics into schedule artifacts.
- [ ] Use bridge/Articulation analysis to identify single‑point‑of‑failure tasks.

---

## Scope 8 — Enhanced Execution Backends (Ray/Dask/Async)

### Objective
Support optional backends beyond local executors for heavy parallelism or IO‑bound workloads.

### Representative code patterns
```python
# src/hamilton_pipeline/driver_factory.py
from hamilton.plugins import h_ray

adapter = h_ray.RayGraphAdapter(ray_init_config={"num_cpus": 8})
builder = builder.with_adapters(adapter)
```

### Target files to modify
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `docs/architecture/part_vi_cpg_build_and_orchestration.md`

### Modules to delete
- None.

### Implementation checklist
- [ ] Add backend selection to execution config surface.
- [ ] Support local Ray/Dask execution for CPU‑bound workloads.
- [ ] Document backend tradeoffs + serialization limitations.

---

## Scope 9 — Result Builders + Materialize Flow

### Objective
Adopt `driver.materialize()` and result builders where appropriate to reduce in‑memory payloads and formalize IO flows.

### Representative code patterns
```python
# src/hamilton_pipeline/execution.py
result = driver_instance.materialize(
    outputs=["write_run_manifest_delta", "write_cpg_nodes_delta"],
    inputs=overrides,
)
```

### Target files to modify
- `src/hamilton_pipeline/execution.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/driver_factory.py`

### Modules to delete
- Legacy output‑collection helpers that only aggregate in‑memory outputs.

### Implementation checklist
- [ ] Convert “save‑only” execution paths to `materialize()`.
- [ ] Ensure run manifest captures materialization outputs deterministically.

---

## Scope 10 — Scheduling Tags + UI Catalog Consistency

### Objective
Standardize node tagging to align with Hamilton UI catalog + filtering. Every node should be tagged with `layer`, `kind`, and `artifact` at minimum; cost/slack/critical‑path tags should be consistent across plan pruning.

### Representative code patterns
```python
# src/hamilton_pipeline/task_module_builder.py
@tag(layer="execution", kind="task", artifact=task.name, task_cost=str(cost))
```

### Target files to modify
- `src/hamilton_pipeline/task_module_builder.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/modules/inputs.py`

### Modules to delete
- None.

### Implementation checklist
- [ ] Define required tag schema and enforce in module builder.
- [ ] Document tag meanings in architecture docs.
- [ ] Ensure consistent tagging for dynamic tasks.

---

## Scope 11 — Deferred Deletions (post‑migration cleanup)

### Objective
Remove legacy patterns only after all scopes above are implemented and validated.

### Delete candidates (defer until all scopes complete)
- Legacy caching helpers that duplicate Hamilton cache stores.
- Hand‑rolled module expansion logic superseded by parameterized sub‑DAGs.
- Any remaining ad‑hoc config branching within node bodies.

### Target files to modify
- `src/hamilton_pipeline/task_module_builder.py`
- `src/hamilton_pipeline/modules/*`
- `src/relspec/*`

### Implementation checklist
- [ ] Audit for redundant logic after new capabilities land.
- [ ] Remove deprecated paths and update tests/goldens.
- [ ] Validate plan artifacts, schema registry, and run manifest stability.

---

## Acceptance Gates
- All modified files pass **ruff**, **pyright**, and **pyrefly** (zero errors).
- Hamilton UI telemetry works with tagged nodes and artifact metadata.
- Cache logs are persisted and traceable to runs.
- rustworkx analytics appear in schedule artifacts and diagnostics.
- Plan signatures remain stable and deterministic after refactors.

