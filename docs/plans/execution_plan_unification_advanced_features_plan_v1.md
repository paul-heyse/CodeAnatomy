# Execution Plan Unification - Advanced Hamilton + rustworkx Features Plan v1 (DataFusion + Delta aligned)

Date: January 27, 2026

## Intent

This plan now serves as the advanced execution/scheduling companion to the
DataFusion + Delta Lake wholesale-switch target state described in
`docs/plans/datafusion_delta_best_in_class_architecture_plan_v1.md`.

The design goal is no longer "make ExecutionPlan nicer in isolation." It is:

- make plan bundles and pinned Delta inputs the only scheduling inputs,
- keep rustworkx as the DAG kernel, but drive it from DataFusion plans,
- use Hamilton dynamic execution as enforcement and observability, not truth,
- treat diagnostics/artifacts as queryable products, and
- keep UI/catalog semantics stable even as plans vary aggressively.

Because we are in design phase, this plan prefers structural leverage and
deterministic correctness boundaries over backward compatibility.

## Target State Invariants (DataFusion + Delta aligned)

1) Session/runtime identity and settings are part of plan identity.
2) `DataFusionPlanBundle` is the canonical scheduling contract.
3) Lineage is structured (no display-string parsing).
4) Delta scan units and pinned versions are first-class DAG nodes.
5) rustworkx remains the graph kernel; Hamilton enforces plan-native rules.
6) Plan artifacts and diagnostics are persisted as queryable tables.

## Status Snapshot (January 27, 2026)

What is already landed and still relevant under the pivot:

- [x] Cache policy defaults now make correctness boundaries explicit.
- [x] Cache lineage export includes metadata-store facts plus decoded
      dependency data versions, and emits `hamilton_cache_lineage_v2`.
- [x] rustworkx transitive reduction, lexicographical ordering, node-link
      JSON, and weighted critical path artifacts are plan-native.
- [x] The scheduler derives generation layers from the reduced dependency DAG.
- [x] Dynamic execution wires plan-native submission and grouping hooks, and
      plan events are flushed as artifacts.
- [x] Plan artifacts now expose plan-native summary nodes via
      `@extract_fields`.
- [x] Plan-vs-runtime drift now emits a `hamilton_plan_drift_v1` artifact.
- [x] UI identity now keeps `dag_name` stable and pushes plan identity into tags.
- [x] ExecutionPlan test stubs were updated to the expanded contract.

Pivot-critical gaps that remain:

- [ ] Session/runtime identity is not yet an explicit planning object.
- [ ] `DataFusionPlanBundle` is not yet enriched with settings, UDF identity,
      rewrite tags, Delta pins, and plan artifacts as first-class fields.
- [ ] Lineage and UDF extraction still rely heavily on display-string parsing.
- [ ] No Delta-aware scan planner/scan units are modeled in the DAG.
- [ ] Plan identity and cache keys do not yet include runtime settings, UDF
      snapshot hash, rewrite tags, or Delta pins.

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
  - [x] allows comparison against plan generations (via drift artifacts).
- [x] Attach hooks in `driver_factory` behind clear config flags.
- [x] Emit diagnostics artifacts that capture:
  - [x] admitted vs rejected tasks,
  - [x] actual groupings,
  - [x] any plan-vs-runtime drift (for example, `hamilton_plan_drift_v1`).

---

## Scope 4 - SessionRuntime + Enriched DataFusionPlanBundle Identity

### Representative architectural patterns

Make runtime/session identity explicit and bind it into plan identity:

```python
from dataclasses import dataclass
from typing import Mapping

from datafusion import SessionContext


@dataclass(frozen=True)
class SessionRuntime:
    ctx: SessionContext
    runtime_hash: str
    udf_snapshot_hash: str
    rewrite_tags: tuple[str, ...]
    df_settings: Mapping[str, str]
```

Enrich the plan bundle so it captures scheduling, reproducibility, and
runtime-compatibility requirements in one canonical surface:

```python
from dataclasses import dataclass
from typing import Mapping

from datafusion.dataframe import DataFrame


@dataclass(frozen=True)
class PlanArtifacts:
    df_settings: Mapping[str, str]
    udf_snapshot_hash: str
    rewrite_tags: tuple[str, ...]
    logical_plan_display: str | None
    optimized_plan_display: str | None
    optimized_plan_graphviz: str | None
    optimized_plan_pgjson: str | None
    execution_plan_display: str | None


@dataclass(frozen=True)
class DeltaInputPin:
    dataset_name: str
    version: int | None
    timestamp: str | None


@dataclass(frozen=True)
class DataFusionPlanBundle:
    df: DataFrame
    plan_fingerprint: str
    artifacts: PlanArtifacts
    delta_inputs: tuple[DeltaInputPin, ...]
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
    plan_details: Mapping[str, object]
```

### Target files to modify

- `src/datafusion_engine/runtime.py`
  - Introduce a first-class `SessionRuntime` surface derived from the profile.
  - Persist a settings snapshot from `information_schema.df_settings`.
- `src/datafusion_engine/plan_bundle.py`
  - Enrich the bundle with plan artifacts, runtime identity, Delta pins, and
    required UDF/rewrite-tag fields.
- `src/datafusion_engine/execution_helpers.py`
  - Update plan and cache key derivations to include runtime identity and pins.
- `src/engine/plan_cache.py`
  - Expand cache keys beyond profile + Substrait hashes.
- `src/datafusion_engine/diagnostics.py`
  - Record bundle/runtime artifacts as first-class, queryable diagnostics.
- `src/relspec/execution_plan.py`
  - Bind plan signatures to enriched bundle identity (not just fingerprints).

### Code and modules to delete

- Display-string fallback fingerprinting once Substrait + settings snapshots
  are required for plan identity.
- Plan cache keys that ignore runtime identity, UDF identity, and Delta pins.

### Implementation checklist

- [x] Runtime profiles already enforce `information_schema=True`.
- [x] Introduce `SessionRuntime` and require it across planning boundaries.
- [ ] Snapshot:
  - [ ] `df_settings`,
  - [ ] `udf_snapshot_hash`,
  - [ ] `rewrite_tags`,
  - [ ] enabled planners/rewrites.
- [ ] Enrich `DataFusionPlanBundle` with:
  - [ ] plan artifacts (display/graphviz/pgjson where available),
  - [ ] runtime artifacts,
  - [ ] Delta input pins,
  - [ ] `required_udfs` and `required_rewrite_tags`.
- [ ] Update plan signatures and cache keys to incorporate:
  - [ ] plan fingerprint,
  - [ ] runtime/session hash,
  - [ ] UDF snapshot hash + rewrite tags,
  - [ ] Delta pins.

---

## Scope 5 - Structured Lineage + Delta Scan Units as First-Class Nodes

### Representative architectural patterns

Lineage must be structured from the plan object, not parsed from display
strings:

```python
from collections.abc import Iterable


def iter_plan_nodes(plan: object) -> Iterable[object]:
    stack = [plan]
    while stack:
        node = stack.pop()
        yield node
        inputs = getattr(node, "inputs", None)
        if callable(inputs):
            stack.extend(inputs())
```

Use Delta metadata and pruning policy to define deterministic scan units:

```python
from dataclasses import dataclass
from pathlib import Path

from storage.deltalake.file_index import build_delta_file_index
from storage.deltalake.file_pruning import FilePruningPolicy, select_candidate_files


@dataclass(frozen=True)
class ScanUnit:
    dataset_name: str
    delta_version: int | None
    candidate_files: tuple[Path, ...]
    projected_columns: tuple[str, ...]
    pushed_filters: tuple[str, ...]


def plan_scan_unit(*, delta_table, lineage) -> ScanUnit:
    index = build_delta_file_index(delta_table)
    policy = FilePruningPolicy(partition_filters=[], stats_filters=[])
    pruned = select_candidate_files(index, policy=policy)
    return ScanUnit(
        dataset_name=lineage.dataset_name,
        delta_version=getattr(delta_table, "version", None),
        candidate_files=tuple(pruned.candidate_paths),
        projected_columns=lineage.projected_columns,
        pushed_filters=lineage.pushed_filters,
    )
```

### Target files to modify

- `src/datafusion_engine/lineage_datafusion.py`
  - Replace display-string parsing with structured traversal (`to_variant`,
    `inputs`, and expression inspection where available).
- `src/datafusion_engine/plan_udf_analysis.py`
  - Replace display parsing with structured UDF extraction.
- `src/datafusion_engine/view_registry_specs.py`
  - Depend on structured lineage outputs (including required UDFs/tags).
- `src/datafusion_engine/view_graph_registry.py`
  - Require structured lineage and enriched bundles for artifact recording.
- `src/relspec/inferred_deps.py`
  - Derive required UDFs/tags and column dependencies from structured lineage.
- `src/storage/deltalake/file_index.py`
  - Ensure index outputs remain stable and compatible with pruning contracts.
- `src/storage/deltalake/file_pruning.py`
  - Provide a planning-grade pruning entrypoint for scan-unit construction.
- `src/relspec/rustworkx_graph.py`
  - Introduce scan-node types and scan-unit edges.
- `src/relspec/execution_plan.py`
  - Compile scan units and bind them into plan signatures and scheduling.

### Code and modules to delete

- Lineage and UDF extraction paths that parse `display_indent*()` output.

### Implementation checklist

- [ ] Implement structured logical-plan traversal and lineage extraction.
- [ ] Emit lineage that includes:
  - [ ] scans, joins, predicates, projections,
  - [ ] required columns per dataset,
  - [ ] required UDFs,
  - [ ] required rewrite tags.
- [x] Introduce scan units with pinned Delta versions.
- [ ] Derive scan units from lineage plus Delta metadata and pruning policy.
- [ ] Bind scan units into plan signatures and cache keys.

---

## Scope 6 - Plan-Driven DAG, Hamilton Enforcement, and Diagnostics as Products

### Representative architectural patterns

Model scans and view computation as distinct node kinds, then reuse the already
landed rustworkx reduction and critical-path artifacts on the unified graph:

```python
from dataclasses import dataclass

import rustworkx as rx


@dataclass(frozen=True)
class TaskNode:
    key: str
    kind: str
    view_name: str | None = None
    dataset_name: str | None = None


def build_task_graph(nodes: list[TaskNode], edges: list[tuple[int, int]]) -> rx.PyDiGraph:
    graph = rx.PyDiGraph(multigraph=False, check_cycle=True)
    graph.add_nodes_from(nodes)
    graph.add_edges_from_no_data(edges)
    return graph
```

Plan artifacts should be first-class Hamilton nodes with stable tagging, while
UI identity remains stable and plan variability is pushed into tags:

```python
from hamilton.function_modifiers import extract_fields, tag


@tag(layer="plan", artifact="plan_artifacts", kind="mapping")
@extract_fields(
    plan_signature=str,
    reduced_plan_signature=str,
    critical_path_task_names=tuple[str, ...],
)
def plan_artifacts(execution_plan) -> dict[str, object]:
    return {
        "plan_signature": execution_plan.plan_signature,
        "reduced_plan_signature": execution_plan.reduced_task_dependency_signature,
        "critical_path_task_names": execution_plan.critical_path_task_names,
    }
```

### Target files to modify

- `src/relspec/rustworkx_graph.py`
  - Extend node models and signatures to include scan units and bundle/runtime
    artifacts as first-class nodes.
- `src/relspec/execution_plan.py`
  - Compile a unified scan + view DAG derived only from structured lineage,
    scan units, and enriched plan bundles.
- `src/relspec/rustworkx_schedule.py`
  - Keep generation and ordering derived only from the reduced unified DAG.
- `src/hamilton_pipeline/scheduling_hooks.py`
  - Enforce plan-native admission and ordering without suppression comments.
  - Emit explicit plan-vs-runtime drift artifacts.
- `src/hamilton_pipeline/driver_factory.py`
  - Use either:
    - local/remote executors, or
    - an `ExecutionManager` profile for scan vs view routing (not both).
  - Stop encoding plan signatures into `hamilton_dag_name`.
- `src/hamilton_pipeline/modules/execution_plan.py`
  - Promote plan artifacts (signatures, scans, lineage, schedule) to explicit
    nodes via `@extract_fields`.
- `src/hamilton_pipeline/semantic_registry.py`
  - Enforce required tags for public semantic outputs.
- `src/datafusion_engine/diagnostics.py`
  - Record plan/runtime/scan artifacts and Hamilton events as queryable tables.

### Code and modules to delete

- `src/hamilton_pipeline/driver_factory.py:config_fingerprint(...)`
- `src/relspec/execution_plan.py:bottom_level_costs(...)` once replaced by
  DAG-kernel-native criticality and/or scan-aware cost models.
- Plan diagnostics duplication once plan artifacts are fully node-driven.
- Plan signatures injected into `dag_name`.

### Implementation checklist

- [x] rustworkx reduction + critical-path artifacts are already plan-native.
- [ ] Unify the DAG around scan units plus plan bundles (scan + view nodes).
- [ ] Ensure all edges come from structured lineage and scan planning only.
- [x] Promote plan artifacts to Hamilton nodes via `@extract_fields`.
- [x] Enforce required semantic tags in registry compilation.
- [x] Remove suppression comments by structurally routing hook inputs.
- [x] Introduce an `ExecutionManager` routing profile for scan-heavy tasks.
- [x] Stabilize `dag_name` and move plan identity into tags.
- [x] Persist plan artifacts and plan events as Delta-backed diagnostics tables.

---

## Scope 7 - Deferred Deletions and Cleanup (Only After Scopes 1-6)

The following deletions should be performed only after the earlier scopes are
complete and validated end-to-end under the DataFusion + Delta architecture.

### Candidate deletions

- Display-string lineage and UDF extraction helpers.
- Legacy plan fingerprint fallbacks that ignore runtime identity.
- Any plan compilation paths that accept builders without emitting a canonical
  enriched plan bundle.
- Hamilton hooks that duplicate node-driven plan artifacts once those nodes
  exist and are validated.

### Deferred deletion checklist

- [ ] Verify candidate functions/modules have no remaining imports.
- [ ] Confirm replacements produce equal-or-better artifacts.
- [ ] Confirm diagnostics sinks and tests are updated to the new artifacts.
- [ ] Remove the legacy code only after all acceptance checks pass.
