# Execution Plan Unification - rustworkx + Hamilton (Best-in-Class Design Plan)

Date: January 26, 2026

## Intent

Unify scheduling, orchestration, and execution around a single compiled
ExecutionPlan that is:

- derived from view definitions and their compiled SQLGlot ASTs,
- scheduled by rustworkx as the graph kernel, and
- injected into Hamilton as the causal contract that drives execution,
  caching, and observability.

This plan opts for the best possible target state, even when it is a breaking
change.

## Target State Invariants

1) The ExecutionPlan is compiled once per run (or per plan signature).
2) The rustworkx schedule is not advisory; it drives what executes.
3) Plan fingerprints and signatures participate in Hamilton cache keys.
4) Scheduling is not computed in multiple places.
5) Observability artifacts are plan-native and deterministic.

## Status Snapshot (January 27, 2026)

- [x] Canonical `ExecutionPlan` compilation exists in `relspec.execution_plan`.
- [x] Driver construction compiles the plan once per build and injects it into
      Hamilton.
- [x] Schedule duplication and generation-wave gating modules have been removed.
- [x] Plan diagnostics, semantic registry, and cache lineage hooks are wired in.
- [x] Dynamic execution uses a plan-aware grouping strategy.
- [ ] Plan-aware submission/grouping lifecycle hooks (priority submission +
      grouping diagnostics) are not yet implemented.
- [x] Driver caching is plan-signature aware and rebuilds drivers when the plan
      changes.
- [x] `active_task_names` now drives runtime gating for inactive tasks.

---

## Scope 1 - Canonical ExecutionPlan Compiler (rustworkx as kernel)

### Representative architectural patterns

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from relspec.evidence import EvidenceCatalog, initial_evidence_from_views, known_dataset_specs
from relspec.inferred_deps import infer_deps_from_view_nodes
from relspec.rustworkx_graph import (
    GraphDiagnostics,
    TaskGraph,
    build_task_graph_from_inferred_deps,
    task_graph_diagnostics,
    task_graph_signature,
    task_graph_snapshot,
)
from relspec.rustworkx_schedule import TaskSchedule, schedule_tasks, task_schedule_metadata

if TYPE_CHECKING:
    from collections.abc import Mapping

    import rustworkx as rx

    from datafusion import SessionContext
    from datafusion_engine.view_graph_registry import ViewNode
    from relspec.schedule_events import TaskScheduleMetadata


@dataclass(frozen=True)
class ExecutionPlan:
    view_nodes: tuple[ViewNode, ...]
    task_graph: TaskGraph
    evidence: EvidenceCatalog
    task_schedule: TaskSchedule
    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    plan_fingerprints: Mapping[str, str]
    plan_signature: str
    diagnostics: GraphDiagnostics
    bottom_level_costs: Mapping[str, float]


def compile_execution_plan(
    *,
    session: SessionContext,
    view_nodes: tuple[ViewNode, ...],
) -> ExecutionPlan:
    inferred = infer_deps_from_view_nodes(view_nodes)
    task_graph = build_task_graph_from_inferred_deps(inferred)
    dataset_specs = known_dataset_specs(ctx=session)
    evidence = initial_evidence_from_views(view_nodes, ctx=session, dataset_specs=dataset_specs)
    schedule = schedule_tasks(task_graph, evidence=evidence, allow_partial=False)
    schedule_meta = task_schedule_metadata(schedule)
    fingerprints = {dep.task_name: dep.plan_fingerprint for dep in inferred}
    snapshot = task_graph_snapshot(
        task_graph,
        label="execution_plan",
        task_signatures=fingerprints,
    )
    signature = task_graph_signature(snapshot)
    diagnostics = task_graph_diagnostics(task_graph, include_node_link=True)
    bottom_costs = _bottom_level_costs(task_graph.graph)
    return ExecutionPlan(
        view_nodes=view_nodes,
        task_graph=task_graph,
        evidence=evidence,
        task_schedule=schedule,
        schedule_metadata=schedule_meta,
        plan_fingerprints=fingerprints,
        plan_signature=signature,
        diagnostics=diagnostics,
        bottom_level_costs=bottom_costs,
    )


def _bottom_level_costs(graph: "rx.PyDiGraph") -> dict[str, float]:
    # Bottom level cost = cost(node) + max(bottom level of successors).
    import rustworkx as rx

    topo = list(rx.topological_sort(graph))
    bottom: dict[int, float] = {}
    for node_idx in reversed(topo):
        succs = graph.successor_indices(node_idx)
        best_succ = max((bottom[s] for s in succs), default=0.0)
        node = graph[node_idx]
        cost = 1.0
        if getattr(node, "kind", None) == "task":
            payload = getattr(node, "payload", None)
            priority = getattr(payload, "priority", 100)
            cost = float(max(priority, 1))
        bottom[node_idx] = cost + best_succ
    out: dict[str, float] = {}
    for node_idx, score in bottom.items():
        node = graph[node_idx]
        if getattr(node, "kind", None) == "task":
            payload = getattr(node, "payload", None)
            name = getattr(payload, "name", None)
            if isinstance(name, str):
                out[name] = score
    return out
```

### Target files to modify

- Add:
  - `src/relspec/execution_plan.py`
- Modify:
  - `src/relspec/inferred_deps.py`
  - `src/relspec/rustworkx_graph.py`
  - `src/relspec/rustworkx_schedule.py`
  - `src/relspec/evidence.py`
  - `src/relspec/__init__.py`
  - `src/datafusion_engine/view_registry_specs.py`
  - `src/datafusion_engine/view_registry.py`

### Code and modules to delete

- None in this scope.

### Implementation checklist

- [x] Create `ExecutionPlan` and `compile_execution_plan(...)` in
      `src/relspec/execution_plan.py`.
- [x] Ensure plan compilation produces:
  - [x] task graph,
  - [x] schedule and per-task schedule metadata,
  - [x] plan fingerprints and plan signature,
  - [x] rustworkx diagnostics artifacts,
  - [x] bottom level costs for prioritization.
- [x] Add plan-level helpers for:
  - [x] impacted task pruning,
  - [x] requested output pruning,
  - [x] evidence seeding from dataset specs.
- [x] Move all plan signature computation behind the plan compiler.

---

## Scope 2 - Plan Injection into Hamilton and Eliminate Schedule Duplication

### Representative architectural patterns

```python
from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType

from hamilton.function_modifiers import tag

from relspec.execution_plan import ExecutionPlan


@dataclass(frozen=True)
class PlanModuleOptions:
    module_name: str = "hamilton_pipeline.generated_plan"


def build_execution_plan_module(plan: ExecutionPlan, *, options: PlanModuleOptions | None = None) -> ModuleType:
    resolved = options or PlanModuleOptions()
    module = ModuleType(resolved.module_name)

    @tag(layer="plan", artifact="execution_plan", kind="object")
    def execution_plan() -> ExecutionPlan:
        return plan

    @tag(layer="plan", artifact="plan_signature", kind="scalar")
    def plan_signature(execution_plan: ExecutionPlan) -> str:
        return execution_plan.plan_signature

    @tag(layer="plan", artifact="task_schedule", kind="schedule")
    def task_schedule(execution_plan: ExecutionPlan):
        return execution_plan.task_schedule

    @tag(layer="plan", artifact="task_generations", kind="schedule")
    def task_generations(execution_plan: ExecutionPlan) -> tuple[tuple[str, ...], ...]:
        return execution_plan.task_schedule.generations

    module.execution_plan = execution_plan
    module.plan_signature = plan_signature
    module.task_schedule = task_schedule
    module.task_generations = task_generations
    module.__all__ = [
        "execution_plan",
        "plan_signature",
        "task_schedule",
        "task_generations",
    ]
    return module
```

```python
from __future__ import annotations

from hamilton import driver

from hamilton_pipeline.task_module_builder import build_task_execution_module
from relspec.execution_plan import compile_execution_plan


def build_driver_from_plan(*, session, view_nodes, base_modules):
    plan = compile_execution_plan(session=session, view_nodes=view_nodes)
    plan_module = build_execution_plan_module(plan)
    task_module = build_task_execution_module(plan=plan)
    return (
        driver.Builder()
        .allow_module_overrides()
        .with_modules(*base_modules, plan_module, task_module)
        .build()
    )
```

### Target files to modify

- Add:
  - `src/hamilton_pipeline/modules/execution_plan.py`
- Modify:
  - `src/hamilton_pipeline/driver_factory.py`
  - `src/hamilton_pipeline/task_module_builder.py`
  - `src/hamilton_pipeline/modules/__init__.py`
  - `src/hamilton_pipeline/modules/task_execution.py`
  - `src/hamilton_pipeline/execution.py`
  - `src/relspec/__init__.py`

### Code and modules to delete

- Schedule duplication inside `src/hamilton_pipeline/driver_factory.py`,
  including:
  - `_build_dependency_map(...)`
  - `_task_graph_metadata(...)`
  - any helper that recomputes schedules outside the plan compiler.

### Implementation checklist

- [x] Build the plan once in `driver_factory` before creating dynamic modules.
- [x] Inject plan nodes via `modules/execution_plan.py`.
- [x] Update `task_module_builder` to accept `ExecutionPlan` instead of
      `dependency_map` and `view_nodes`.
- [x] Ensure schedule metadata and fingerprints come from the injected plan.
- [x] Remove all schedule recomputation paths outside the plan compiler.
- [x] Make driver caching plan-sensitive by including the plan signature (or a
      plan fingerprint) in the driver cache key.

---

## Scope 3 - Scheduler-Driven Pruning, Priority, and Dynamic Execution Hooks

### Representative architectural patterns

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hamilton.lifecycle import api as lifecycle_api

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from relspec.execution_plan import ExecutionPlan
    from relspec.schedule_events import TaskScheduleMetadata


@dataclass(frozen=True)
class PlanSchedulingContext:
    schedule_metadata: Mapping[str, TaskScheduleMetadata]
    bottom_level_costs: Mapping[str, float]


class GenerationGroupingHook(lifecycle_api.TaskGroupingHook):
    def __init__(self, ctx: PlanSchedulingContext) -> None:
        self._ctx = ctx

    def post_task_group(self, *, tasks: Iterable[lifecycle_api.TaskSpec], **kwargs):
        grouped: dict[int, list[lifecycle_api.TaskSpec]] = {}
        for task in tasks:
            meta = self._ctx.schedule_metadata.get(task.base_id)
            generation = meta.generation_index if meta is not None else 0
            grouped.setdefault(generation, []).append(task)
        return [lifecycle_api.TaskGroup(group_id=str(gen), tasks=items) for gen, items in sorted(grouped.items())]


class PrioritySubmissionHook(lifecycle_api.TaskSubmissionHook):
    def __init__(self, ctx: PlanSchedulingContext) -> None:
        self._ctx = ctx

    def pre_task_submission(self, *, tasks: Iterable[lifecycle_api.TaskSpec], **kwargs):
        return sorted(
            tasks,
            key=lambda task: self._ctx.bottom_level_costs.get(task.base_id, 0.0),
            reverse=True,
        )
```

### Target files to modify

- Add:
  - `src/hamilton_pipeline/scheduling_hooks.py`
- Modify:
  - `src/hamilton_pipeline/driver_factory.py`
  - `src/hamilton_pipeline/lifecycle.py`
  - `src/hamilton_pipeline/task_module_builder.py`
  - `src/hamilton_pipeline/modules/task_execution.py`
  - `src/relspec/execution_plan.py`

### Code and modules to delete

- Generation gating path that executes waves outside per-task nodes:
  - `task_generation_wave(...)`
  - `generation_outputs(...)`
  - `generation_outputs_all(...)`
  - `generation_execution_gate(...)`
- Generation gate injection inside `task_module_builder`.

### Implementation checklist

- [x] Prune tasks at plan compile time (impact and requested outputs).
- [x] Ensure only pruned tasks are present in the generated task module.
- [x] Remove generation-wave gating in favor of per-task execution nodes.
- [x] Use plan schedule metadata for generation-aware grouping.
- [ ] Add plan-aware `TaskSubmissionHook` and/or `TaskGroupingHook` so ready
      work is ordered by bottom-level criticality at submission time.
- [ ] Wire scheduling hooks into dynamic execution adapters and record their
      diagnostics artifacts.
- [x] Keep rustworkx as the scheduling source of truth.
- [x] Enforce runtime gating so tasks outside `active_task_names` are skipped,
      not executed.
- [x] Consider pruning generated task nodes further when incremental diff limits
      the active closure.

---

## Scope 4 - Plan-Aware Caching, Lineage, and Invalidation Correctness

### Representative architectural patterns

```python
from __future__ import annotations

from hamilton.caching.stores.file import FileResultStore
from hamilton.caching.stores.sqlite import SQLiteMetadataStore


def apply_cache(builder, *, cache_dir: str):
    metadata = SQLiteMetadataStore(path=f"{cache_dir}/meta.sqlite")
    results = FileResultStore(path=f"{cache_dir}/results")
    return builder.with_cache(
        metadata_store=metadata,
        result_store=results,
        default_behavior="disable",
        log_to_file=True,
    )
```

```python
from __future__ import annotations

from hamilton.function_modifiers import inject, source, value


def inject_plan_inputs(node_fn, *, task_name: str, task_output: str, plan_fingerprint: str):
    return inject(
        task_name=value(task_name),
        task_output=value(task_output),
        plan_fingerprint=value(plan_fingerprint),
        plan_signature=source("plan_signature"),
    )(node_fn)
```

### Target files to modify

- Add:
  - `src/hamilton_pipeline/cache_lineage.py`
- Modify:
  - `src/hamilton_pipeline/driver_factory.py`
  - `src/hamilton_pipeline/task_module_builder.py`
  - `src/relspec/execution_plan.py`
  - `src/obs/diagnostics.py`
  - `src/datafusion_engine/diagnostics.py`

### Code and modules to delete

- Any plan fingerprint or plan signature tags that are not causal inputs
  to Hamilton nodes.

### Implementation checklist

- [x] Inject `plan_signature` and per-task `plan_fingerprint` as inputs.
- [x] Configure cache with opt-in defaults and JSONL logs.
- [x] Export deterministic cache lineage per run.
- [x] Record cache lineage and plan signature in diagnostics artifacts.

---

## Scope 5 - Unified Tag Taxonomy and Plan-Native Observability Artifacts

### Representative architectural patterns

```python
from __future__ import annotations

from hamilton.function_modifiers import tag


@tag(
    layer="semantic",
    semantic_id="cpg.nodes.v1",
    kind="table",
    entity="node",
    grain="per_node",
    version="1",
    stability="stable",
    schema_ref="semantic.cpg_nodes_v1",
    entity_keys="repo,commit,node_id",
    join_keys="repo,commit,node_id",
    materialization="delta",
    materialized_name="semantic.cpg_nodes_v1",
)
def cpg_nodes(...):
    ...
```

```python
from __future__ import annotations

from datafusion_engine.diagnostics import record_artifact
from relspec.execution_plan import ExecutionPlan


def record_plan_diagnostics(profile, plan: ExecutionPlan) -> None:
    diagnostics = plan.diagnostics
    record_artifact(
        profile,
        "task_graph_diagnostics_v2",
        {
            "plan_signature": plan.plan_signature,
            "critical_path": list(diagnostics.critical_path or ()),
            "critical_path_length": diagnostics.critical_path_length,
            "weak_components": [list(c) for c in diagnostics.weak_components],
            "isolates": list(diagnostics.isolates),
            "dot": diagnostics.dot,
            "node_link_json": diagnostics.node_link_json,
        },
    )
```

### Target files to modify

- Add:
  - `src/hamilton_pipeline/semantic_registry.py`
- Modify:
  - `src/hamilton_pipeline/modules/outputs.py`
  - `src/hamilton_pipeline/driver_factory.py`
  - `src/datafusion_engine/diagnostics.py`
  - `src/obs/diagnostics.py`
  - `src/relspec/execution_plan.py`

### Code and modules to delete

- Ad hoc tags that overlap with plan schedule metadata but do not follow
  a consistent taxonomy.

### Implementation checklist

- [x] Adopt a semantic tag contract for public outputs.
- [x] Ensure schedule metadata tags come from the plan.
- [x] Emit plan diagnostics artifacts for every run.
- [x] Optionally compile a semantic registry from tags as a first-class artifact.

---

## Scope 6 - Deferred Deletions (only after all scopes above are complete)

These deletions should be performed only after the plan compiler, plan
injection, scheduler-driven execution, and plan-aware caching are all in place.

### Candidate deletions

- `src/hamilton_pipeline/modules/task_graph.py`
  - Replaced by plan injection and plan-derived schedule nodes.
- `src/hamilton_pipeline/modules/incremental_plan.py`
  - Replace with plan compilation that includes incremental diff logic
    and post-run snapshot persistence.
- Legacy schedule duplication helpers in `src/hamilton_pipeline/driver_factory.py`.
- Generation wave gating path in `src/hamilton_pipeline/modules/task_execution.py`
  if any portion remains.
- Thin wrappers that exist only to support the previous multi-path plan flow,
  such as:
  - `src/relspec/graph_inference.py` (if fully subsumed by plan compile),
  - `src/relspec/incremental.py:view_fingerprint_map` and
    `src/relspec/incremental.py:view_snapshot_map` (if replaced by plan artifacts).
- Tests that encode the old orchestration shape, such as:
  - `tests/unit/test_incremental_plan_gating.py`
  - task graph tests that assume schedule recomputation inside Hamilton modules.

### Deferred deletion checklist

- [x] Verify no remaining imports reference the candidate modules.
- [x] Replace tests with plan-centric scheduling and execution tests.
- [x] Confirm plan signature changes invalidate cached results as intended.
- [ ] Confirm dynamic execution uses plan submission/grouping hooks in addition
      to the grouping strategy.
