# Execution Plan Unification - Advanced Hamilton + rustworkx Features Plan v1

Date: January 27, 2026

## Intent

This plan extends the already-unified ExecutionPlan architecture to a
best-in-class endpoint that is:

- maximally causal for caching and invalidation correctness,
- explicitly DAG-kernel-driven via rustworkx,
- enforced at runtime through Hamilton dynamic execution hooks,
- deeply observable through lineage-grade diagnostics, and
- UI/catalog friendly without exploding DAG version churn.

Because we are in the design phase, this plan prefers structural clarity,
correctness boundaries, and system-level leverage, even when it introduces
breaking changes.

## Target State Invariants

1) Cache correctness boundaries are explicit and enforced by policy.
2) rustworkx is the graph kernel for both planning and scheduling semantics.
3) Runtime admission control is plan-native (not advisory).
4) Plan diagnostics are first-class nodes and first-class artifacts.
5) UI/telemetry semantics are stable, queryable, and governance-friendly.

## Status Snapshot (January 27, 2026)

- [x] Cache policy defaults now make correctness boundaries explicit.
- [x] Cache lineage export now includes metadata-store facts and decoded
      dependency data versions.
- [x] rustworkx transitive reduction and weighted critical path are now
      plan-native artifacts.
- [x] The scheduler now derives generation layers from the reduced dependency
      DAG.
- [x] Dynamic execution now wires plan-native submission and grouping hooks.
- [ ] Plan artifacts are not yet first-class Hamilton nodes.
- [ ] UI identity still encodes the plan signature into the DAG name.
- [ ] Tests and plan stubs have not yet been updated to the expanded
      `ExecutionPlan` contract.

---

## Scope 1 - Caching Correctness Boundary + Lineage-Grade Cache Facts

### Representative architectural patterns

Plan-aware caching policy that treats external reads as recompute boundaries,
while still emitting structured logs and causal lineage:

```python
from hamilton import driver
from hamilton.caching.stores.file import FileResultStore
from hamilton.caching.stores.sqlite import SQLiteMetadataStore


def apply_cache(builder: driver.Builder, *, cache_dir: str) -> driver.Builder:
    metadata = SQLiteMetadataStore(path=f"{cache_dir}/meta.sqlite")
    results = FileResultStore(path=f"{cache_dir}/results")
    return builder.with_cache(
        metadata_store=metadata,
        result_store=results,
        default_behavior="disable",
        default_loader_behavior="recompute",
        default_saver_behavior="disable",
        log_to_file=True,
    )
```

Two-phase lineage export: (1) structured cache logs, and (2) authoritative
cache metadata with decoded dependency data versions:

```python
from hamilton.caching.cache_key import decode_key


def lineage_rows_from_metadata_store(cache, *, run_id: str) -> list[dict[str, object]]:
    run_meta = cache.metadata_store.get_run(run_id)
    rows: list[dict[str, object]] = []
    for node_name, meta in sorted(run_meta.nodes.items()):
        cache_key = meta.cache_key
        decoded = decode_key(cache_key) if isinstance(cache_key, str) else {}
        rows.append(
            {
                "run_id": run_id,
                "node_name": node_name,
                "cache_key": cache_key,
                "data_version": meta.data_version,
                "code_version": meta.code_version,
                "dependencies_data_versions": decoded.get("dependencies_data_versions", {}),
            }
        )
    return rows
```

### Target files to modify

- `src/hamilton_pipeline/driver_factory.py`
  - Promote loader/saver default behaviors to explicit correctness boundaries.
  - Make cache policy a named, testable profile (for example, "strict causal").
- `src/hamilton_pipeline/cache_lineage.py`
  - Add metadata-store ingestion and decode `cache_key` into dependency facts.
  - Emit `hamilton_cache_lineage_v2` with per-node lineage fields.
- `src/hamilton_pipeline/modules/inputs.py`
  - Confirm `@cache(behavior="ignore")` is applied to all ephemeral/runtime
    infrastructure nodes (context, sessions, clients, backends).
- `src/datafusion_engine/diagnostics.py`
  - Provide helpers to record per-node cache lineage facts as structured
    artifacts, not just run summaries.
- `src/obs/diagnostics.py`
  - Optionally add cache lineage ingestion helpers for local analysis sinks.

### Code and modules to delete

- None in this scope. (Deferred cleanup is listed at the end.)

### Implementation checklist

- [x] Add explicit cache defaults:
  - [x] `default_behavior="disable"`,
  - [x] `default_loader_behavior="recompute"`,
  - [x] `default_saver_behavior="disable"`,
  - [x] `log_to_file=True`.
- [x] Implement metadata-store lineage export that includes:
  - [x] `cache_key`,
  - [x] `data_version`,
  - [x] `code_version`,
  - [x] `dependencies_data_versions` via `decode_key(cache_key)`.
- [x] Record lineage facts as a first-class artifact (for example,
      `hamilton_cache_lineage_v2`) and include the plan signature.
- [ ] Add targeted tests that assert:
  - [ ] external reads are recomputed even when cached,
  - [x] `plan_signature` participates in dependency data versions,
  - [ ] metadata-store lineage rows are deterministic and decode cleanly.

---

## Scope 2 - rustworkx as the DAG Kernel for Planning + Scheduling Semantics

### Representative architectural patterns

Transitive reduction at plan-compile time so the scheduling graph is minimal
but reachability-equivalent:

```python
import rustworkx as rx


def reduce_dag(graph: rx.PyDiGraph) -> tuple[rx.PyDiGraph, dict[int, int]]:
    reduced, node_map = rx.transitive_reduction(graph)
    return reduced, dict(node_map)
```

Critical path and criticality derived from rustworkx DAG algorithms (not custom
graph traversal logic):

```python
import rustworkx as rx


def task_weight(node: object) -> float:
    payload = getattr(node, "payload", None)
    priority = getattr(payload, "priority", 100)
    return float(max(priority, 1))


def critical_path(graph: rx.PyDiGraph) -> tuple[int, ...]:
    return tuple(rx.dag_weighted_longest_path(graph, weight_fn=task_weight))
```

### Target files to modify

- `src/relspec/rustworkx_graph.py`
  - Move transitive reduction from diagnostics-only into plan-facing artifacts.
  - Emit both the full graph signature and reduced-graph signature.
  - Carry reduction `node_map` as a plan artifact.
- `src/relspec/rustworkx_schedule.py`
  - Ensure generation/layer computations are derived from the reduced graph.
  - Prefer rustworkx layering helpers (for example,
    `topological_generations` / `layers`) as the only source of truth.
- `src/relspec/execution_plan.py`
  - Compile plan diagnostics and scheduling against the reduced DAG kernel.
  - Replace custom criticality logic with rustworkx DAG algorithms where
    possible and make any remaining custom logic explicitly plan-scoped.
- `src/hamilton_pipeline/scheduling_hooks.py`
  - Consume rustworkx-derived criticality/critical-path artifacts rather than
    recomputing task priority semantics locally.
- `src/hamilton_pipeline/lifecycle.py`
  - Record reduced-graph diagnostics and critical-path artifacts explicitly.

### Code and modules to delete

- None immediately. (See deferred deletions at the end for replacements such
  as custom bottom-level cost logic.)

### Implementation checklist

- [x] Promote transitive reduction into plan compilation:
  - [x] compile and store reduced graph artifacts,
  - [x] store reduction `node_map` for diagnostics and traceability.
- [x] Ensure schedule metadata is derived from the reduced DAG kernel.
- [x] Use rustworkx DAG algorithms directly for:
  - [x] critical path extraction,
  - [x] critical path length,
  - [x] reachability-preserving reductions.
- [x] Ensure plan diagnostics artifacts include:
  - [x] full graph signature,
  - [x] reduced graph signature,
  - [x] reduction node map,
  - [x] critical path over the chosen scheduling graph.

---

## Scope 3 - Runtime Admission Control via Dynamic Execution Hooks

### Representative architectural patterns

Plan-native admission control and ordering at task submission time:

```python
from hamilton.lifecycle import api as lifecycle_api


class PlanTaskSubmissionHook(lifecycle_api.TaskSubmissionHook):
    def __init__(self, *, active_tasks: set[str], criticality: dict[str, float]) -> None:
        self._active = active_tasks
        self._criticality = criticality

    def pre_task_submission(self, *, tasks, **kwargs):
        _ = kwargs
        admitted = [task for task in tasks if task.base_id in self._active]
        admitted.sort(key=lambda task: self._criticality.get(task.base_id, 0.0), reverse=True)
        return admitted
```

Task grouping hook that records actual groupings for diagnostics and drift
analysis:

```python
from hamilton.lifecycle import api as lifecycle_api


class PlanTaskGroupingHook(lifecycle_api.TaskGroupingHook):
    def __init__(self, recorder) -> None:
        self._recorder = recorder

    def post_task_group(self, *, run_id, task_groups, **kwargs):
        _ = kwargs
        self._recorder(run_id=run_id, task_groups=task_groups)
        return task_groups
```

### Target files to modify

- `src/hamilton_pipeline/scheduling_hooks.py`
  - Add `TaskSubmissionHook` and (optionally) `TaskGroupingHook` implementations
    that enforce plan-native runtime semantics.
- `src/hamilton_pipeline/driver_factory.py`
  - Attach the new task hooks via `Builder.with_adapters(...)`.
  - Optionally introduce an `ExecutionManager` profile that routes heavy tasks
    to remote executors and light tasks locally.
- `src/hamilton_pipeline/lifecycle.py`
  - Record admission-control decisions and actual grouped execution for
    diagnostics artifacts.
- `src/relspec/execution_plan.py`
  - Expose any additional plan-native scheduling diagnostics needed by hooks.

### Code and modules to delete

- None in this scope.

### Implementation checklist

- [x] Implement `PlanTaskSubmissionHook` that:
  - [x] filters out tasks not in `active_task_names`,
  - [ ] enforces plan-native ordering via criticality,
  - [x] is explicit about fallback behavior for unknown tasks.
- [x] Implement (or stub) a `PlanTaskGroupingHook` that:
  - [x] records actual groupings,
  - [ ] allows comparison against plan generations.
- [x] Attach hooks in `driver_factory` behind clear config flags.
- [x] Emit diagnostics artifacts that capture:
  - [x] admitted vs rejected tasks,
  - [x] actual groupings,
  - [ ] any plan-vs-runtime drift.

---

## Scope 4 - Plan Artifacts as First-Class Nodes (Extract + Validate + Tag)

### Representative architectural patterns

Split plan diagnostics into explicit nodes that are individually cacheable,
queryable, and testable:

```python
from hamilton.function_modifiers import extract_fields, tag


@tag(layer="plan", artifact="plan_diagnostics", kind="mapping")
@extract_fields(
    plan_critical_path=tuple[str, ...],
    plan_isolates=tuple[str, ...],
    plan_reduction_node_map=dict,
)
def plan_diagnostics(execution_plan) -> dict[str, object]:
    diagnostics = execution_plan.diagnostics
    return {
        "plan_critical_path": tuple(diagnostics.critical_path or ()),
        "plan_isolates": tuple(diagnostics.isolate_labels),
        "plan_reduction_node_map": dict(diagnostics.node_map or {}),
    }
```

Validation that is architectural (not ad hoc), using Hamilton validators:

```python
from hamilton.function_modifiers import check_output, tag


@tag(layer="semantic", semantic_id="semantic.cpg_nodes_v1", kind="table")
@check_output(importance="fail", allow_nans=False)
def cpg_nodes(cpg_nodes_final):
    return cpg_nodes_final
```

### Target files to modify

- `src/hamilton_pipeline/modules/execution_plan.py`
  - Introduce extracted plan diagnostics nodes (critical path, isolates,
    reduction maps, schedule tables, and so on).
  - Ensure plan artifacts are tagged consistently and include the plan signature.
- `src/hamilton_pipeline/lifecycle.py`
  - Prefer plan-artifact nodes as diagnostic sources where appropriate, and
    keep hooks focused on run framing (run_id, timestamps, envelope metadata).
- `src/hamilton_pipeline/modules/outputs.py`
  - Expand validation beyond non-empty checks to include structural constraints
    that match the semantic contract.
- `src/hamilton_pipeline/semantic_registry.py`
  - Upgrade registry compilation into a validator with explicit required-tag
    enforcement for public semantic outputs.
- `src/datafusion_engine/diagnostics.py`
  - Add helpers to record plan-artifact nodes as first-class diagnostics
    artifacts in a consistent schema.

### Code and modules to delete

- None immediately. (See deferred deletions at the end for hook cleanup once
  plan artifacts fully subsume existing diagnostics hooks.)

### Implementation checklist

- [ ] Create plan-diagnostics nodes using `@extract_fields` so individual plan
      artifacts are addressable nodes.
- [ ] Ensure all plan artifacts are tagged with:
  - [ ] `layer="plan"`,
  - [ ] stable `artifact` identifiers,
  - [ ] the plan signature where appropriate.
- [ ] Strengthen semantic output validation with `@check_output` and/or
      `@check_output_custom` where contracts are known.
- [ ] Enforce required semantic tags via registry validation and fail fast on
      malformed public outputs.

---

## Scope 5 - UI/Telemetry Semantics that Scale (Stable DAG Names + Rich Tags)

### Representative architectural patterns

Keep DAG version identity stable while surfacing plan variability in tags:

```python
def ui_identity(config: dict[str, object], *, plan_signature: str) -> tuple[str, dict[str, str]]:
    dag_name = str(config.get("hamilton_dag_name") or "codeintel_semantic_v1")
    tags = {
        "plan_signature": plan_signature,
        "plan_task_count": str(config.get("plan_task_count", "")),
        "plan_generation_count": str(config.get("plan_generation_count", "")),
        "semantic_version": "v1",
    }
    return dag_name, tags
```

Governance-friendly capture defaults:

```bash
export HAMILTON_CAPTURE_DATA_STATISTICS=0
export HAMILTON_MAX_LIST_LENGTH_CAPTURE=20
export HAMILTON_MAX_DICT_LENGTH_CAPTURE=50
```

### Target files to modify

- `src/hamilton_pipeline/driver_factory.py`
  - Stop encoding the plan signature into `dag_name`.
  - Keep `dag_name` stable and push plan identity into tags.
  - Enrich tracker tags with plan-level metadata (task counts, generation
    counts, semantic version, and so on).
- `src/hamilton_pipeline/semantic_registry.py`
  - Align semantic registry payloads with the UI-facing tag taxonomy.
- `src/datafusion_engine/diagnostics.py`
  - Ensure run-level artifacts include tracker-compatible plan metadata.

### Code and modules to delete

- None in this scope.

### Implementation checklist

- [ ] Make `dag_name` stable and semantic-version scoped.
- [ ] Push plan variability into tags (plan signature, counts, fingerprints).
- [ ] Ensure tracker configuration is governance-friendly by default and easy
      to clamp further in production environments.
- [ ] Validate that registry payloads and tracker tags are aligned (so the UI
      becomes a reliable catalog, not just a telemetry stream).

---

## Scope 6 - Deferred Deletions and Cleanup (Only After Scopes 1-5)

The following deletions should be performed only after the earlier scopes
are complete and validated end-to-end.

### Candidate deletions

- `src/hamilton_pipeline/driver_factory.py:config_fingerprint(...)`
  - Now unused after plan-aware caching keys.
- `src/relspec/execution_plan.py:bottom_level_costs(...)`
  - Replace with rustworkx DAG-kernel-native criticality artifacts.
- Diagnostics duplication once plan artifacts are fully first-class:
  - `PlanDiagnosticsHook` can potentially be reduced to a thin run envelope
    (or removed) once plan diagnostics are exclusively node-driven.
- Any local recomputation of plan diagnostics that becomes redundant once
  reduced-graph and critical-path artifacts are plan-native.

### Deferred deletion checklist

- [ ] Verify candidate functions/modules have no remaining imports.
- [ ] Confirm replacements produce equal-or-better artifacts.
- [ ] Confirm diagnostics sinks and tests are updated to the new artifacts.
- [ ] Remove the legacy code only after all acceptance checks pass.
