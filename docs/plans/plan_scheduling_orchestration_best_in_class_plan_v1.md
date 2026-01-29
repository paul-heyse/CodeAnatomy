# Plan Scheduling, Orchestration, and Execution Best-in-Class Plan (Design-Phase, Breaking Changes OK)

Date: 2026-01-29  
Owner: Codex (design phase)  
Status: complete (Scopes 1–11 implemented; design-phase target state reached)  
Scope: rustworkx scheduling + Hamilton orchestration + msgspec artifacts

## Purpose
Define the best‑in‑class target state for plan scheduling, orchestration, and execution by aligning
rustworkx and Hamilton with the new deterministic plan artifacts, stats‑aware cost model, and
msgspec‑first diagnostics. This plan is intentionally ambitious and may include breaking changes to
reach the optimal end state.

## Design Principles (Non‑Negotiable)
- **Plan‑aware execution is first‑class**: runtime orchestration must explicitly consume plan
  signatures, task costs, and schedule metadata.
- **Stats‑aware scheduling**: DataFusion metrics and plan statistics drive cost models, critical path,
  and task ordering.
- **Artifacts are contracts**: schedule and validation diagnostics are typed msgspec envelopes with
  schema exports and golden tests.
- **Parallelism is explicit and configurable**: dynamic execution is opt‑in by API, not hidden in
  loose config maps.
- **Breaking changes are acceptable**: prioritize correctness, observability, and reproducibility.

---

## Scope 1 — Public Execution Mode + Executor Config (first‑class API)

### Objective
Expose execution mode, dynamic execution, and executor configuration via the public API rather
than hidden config flags, and make parallel scheduling the default for production builds.

### Representative code patterns
```python
# src/graph/product_build.py
class ExecutionMode(str, Enum):
    DETERMINISTIC_SERIAL = "deterministic_serial"
    PLAN_PARALLEL = "plan_parallel"
    PLAN_PARALLEL_REMOTE = "plan_parallel_remote"

@dataclass(frozen=True)
class ExecutorConfig:
    kind: Literal["threadpool", "multiprocessing", "dask", "ray"] = "multiprocessing"
    max_tasks: int = 4
    remote_kind: Literal["threadpool", "multiprocessing", "dask", "ray"] | None = None

@dataclass(frozen=True)
class GraphProductBuildRequest:
    execution_mode: ExecutionMode = ExecutionMode.PLAN_PARALLEL
    executor_config: ExecutorConfig | None = None
```

```python
# src/hamilton_pipeline/driver_factory.py
builder = builder.with_modules(*modules).with_config(config_payload)
builder = _apply_dynamic_execution(
    builder,
    options=DynamicExecutionOptions(
        config=config_payload,
        plan=plan,
        diagnostics=diagnostics,
        execution_mode=execution_mode,
        executor_config=executor_config,
    ),
)
```

### Target files to modify
- `src/graph/product_build.py`
- `src/hamilton_pipeline/execution.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/driver_factory.py`
- `scripts/run_full_pipeline.py`
- `docs/architecture/part_vi_cpg_build_and_orchestration.md`

### Modules to delete
- None.

### Implementation checklist
- [x] Add `ExecutionMode` + `ExecutorConfig` to public request types.
- [x] Thread `execution_mode` and `executor_config` into Hamilton driver construction.
- [x] Make `plan_parallel` the default in production workflows.
- [x] Update CLI to reflect the new execution surface.
- [x] Update architecture docs to reflect the new execution surface.

---

## Scope 2 — Cost‑Aware Critical Path and Slack‑Aware Ordering

### Objective
Drive critical path, slack, and per‑generation ordering from the DataFusion cost model instead of
priority‑only heuristics.

### Representative code patterns
```python
# src/relspec/rustworkx_graph.py
def task_dependency_critical_path(
    graph: rx.PyDiGraph,
    *,
    task_costs: Mapping[str, float],
) -> tuple[int, ...]:
    def _edge_weight(_source: int, target: int, _edge: object) -> float:
        task_name = _task_node_name(graph[target])
        return float(max(task_costs.get(task_name, 1.0), 1.0))
    return tuple(rx.dag_weighted_longest_path(graph, _edge_weight))
```

```python
# src/relspec/rustworkx_schedule.py
def schedule_tasks(
    graph: TaskGraph,
    *,
    evidence: EvidenceCatalog,
    options: ScheduleOptions | None = None,
) -> TaskSchedule:
    ...
    cost_context = resolved_options.cost_context
    ready_tasks.sort(
        key=lambda task: _task_sort_key(task, cost_context=cost_context),
    )
```

### Target files to modify
- `src/relspec/rustworkx_graph.py`
- `src/relspec/rustworkx_schedule.py`
- `src/relspec/execution_plan.py`
- `src/hamilton_pipeline/scheduling_hooks.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add cost‑aware critical path functions with task‑cost injection.
- [x] Compute slack per task (bottom‑level vs earliest/latest) and expose it in diagnostics.
- [x] Update schedule ordering to use bottom‑level cost + slack + name fallback.
- [x] Ensure deterministic ordering when costs tie.

---

## Scope 3 — Plan Schedule & Validation Artifacts v2 (msgspec bytes / structured columns)

### Objective
Create canonical msgspec artifacts for plan scheduling and edge validation, stored as **structured
Arrow columns or msgspec bytes** (no JSON string columns). Keep the run manifest lean while linking
to these artifacts by ID.

### Representative code patterns
```python
# src/serde_artifacts.py
class PlanScheduleArtifact(StructBaseCompat, frozen=True):
    run_id: RunId
    plan_signature: str
    reduced_plan_signature: str
    task_count: NonNegInt
    ordered_tasks: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    critical_path_tasks: tuple[str, ...]
    critical_path_length_weighted: NonNegFloat | None
    task_costs: dict[str, NonNegFloat]
    bottom_level_costs: dict[str, NonNegFloat]
    slack_by_task: dict[str, NonNegFloat] | None = None

class PlanScheduleEnvelope(ArtifactEnvelopeBase, tag="plan_schedule", frozen=True):
    payload: PlanScheduleArtifact
```

```python
# src/datafusion_engine/plan_artifact_store.py
envelope = PlanScheduleEnvelope(payload=schedule_payload)
row = {
    "event_name": "plan_schedule_v1",
    "event_payload_msgpack": dumps_msgpack(envelope),
    "event_payload_hash": payload_hash(envelope),
}
persist_hamilton_events_v2(..., rows=[row])
```

### Target files to modify
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/hamilton_pipeline/lifecycle.py`
- `src/hamilton_pipeline/plan_artifacts.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `tests/msgspec_contract/test_contract_*.py`
- `tests/unit/test_plan_bundle_determinism_audit.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add `PlanScheduleArtifact` + envelope to msgspec contracts.
- [x] Add `PlanValidationArtifact` + envelope for edge validation results.
- [x] Define structured/msgspec schema for schedule + validation artifacts (no JSON columns).
- [x] Emit schedule + validation artifacts as msgspec bytes (Hamilton events v2).
- [x] Link artifact IDs in the run manifest for traceability.
- [x] Add msgspec golden tests (JSON/MsgPack/schema/errors).

---

## Scope 4 — Hamilton Events v2 (structured payloads, no JSON columns)

### Objective
Replace `datafusion_hamilton_events_v1` JSON payload columns with msgspec bytes or structured
fields and **hard cut over** to v2 (no dual‑write/backfill). Schedule/validation artifacts must flow
through v2 only.

### Representative code patterns
```python
# src/datafusion_engine/schema_registry.py
DATAFUSION_HAMILTON_EVENTS_V2_SCHEMA = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("profile_name", pa.string(), nullable=True),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("event_name", pa.string(), nullable=False),
        pa.field("plan_signature", pa.string(), nullable=False),
        pa.field("reduced_plan_signature", pa.string(), nullable=False),
        pa.field("event_payload_msgpack", pa.binary(), nullable=False),
        pa.field("event_payload_hash", pa.string(), nullable=False),
    ]
)
```

```python
# src/datafusion_engine/plan_artifact_store.py
def persist_hamilton_events_v2(..., rows: Sequence[dict[str, object]]) -> None:
    table = pa.Table.from_pylist(rows, schema=DATAFUSION_HAMILTON_EVENTS_V2_SCHEMA)
    _write_artifact_table(..., table)
```

### Target files to modify
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/hamilton_pipeline/lifecycle.py`
- `src/incremental/registry_rows.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to delete
- `datafusion_hamilton_events_v1` (immediate cutover)
- `event_payload_json` column in legacy schema

### Implementation checklist
- [x] Define `datafusion_hamilton_events_v2` schema with msgspec bytes.
- [x] Cut over writers/readers to v2 only (no dual‑write).
- [x] Remove v1 table, registration, and JSON payload column.

---

## Scope 5 — Validation Diagnostics for Evidence Edges

### Objective
Persist detailed evidence validation results (missing columns/types/metadata and contract
violations) as a typed artifact and use it in admission control.

### Representative code patterns
```python
# src/relspec/graph_edge_validation.py
def validate_graph(
    graph: TaskGraph,
    *,
    catalog: EvidenceCatalog,
) -> GraphValidationSummary:
    ...

# src/serde_artifacts.py
class PlanValidationArtifact(StructBaseCompat, frozen=True):
    run_id: RunId
    plan_signature: str
    invalid_tasks: NonNegInt
    invalid_edges: NonNegInt
    task_results: tuple[dict[str, JsonValue], ...]
```

### Target files to modify
- `src/relspec/graph_edge_validation.py`
- `src/relspec/rustworkx_schedule.py`
- `src/serde_artifacts.py`
- `src/hamilton_pipeline/lifecycle.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add graph‑level validation summary builder.
- [x] Emit validation artifacts prior to scheduling admission.
- [x] Surface validation payload in diagnostics and plan artifacts.

---

## Scope 6 — Plan‑Aware Tags for Hamilton Nodes

### Objective
Tag execution nodes with plan cost, slack, critical‑path membership, and schedule indices so
Hamilton adapters and UI can consume them without re‑hydrating the full plan.

### Representative code patterns
```python
# src/hamilton_pipeline/task_module_builder.py
return tag(
    layer="execution",
    kind="task",
    task_name=task.name,
    task_cost=str(task_costs.get(task.name, 0.0)),
    bottom_level_cost=str(bottom_costs.get(task.name, 0.0)),
    slack=str(slack_by_task.get(task.name, 0.0)),
    on_critical_path=str(task.name in critical_path),
    schedule_index=schedule_tags["schedule_index"],
    generation_index=schedule_tags["generation_index"],
)(node_fn)
```

### Target files to modify
- `src/hamilton_pipeline/task_module_builder.py`
- `src/hamilton_pipeline/scheduling_hooks.py`
- `src/relspec/execution_plan.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add cost/slack/critical‑path tags to execution nodes.
- [x] Ensure tags are consistent across plan pruning and dynamic execution.

---

## Scope 7 — Plan‑Aware Executor Routing (cost + kind)

### Objective
Route tasks to local/remote executors based on task kind **and** computed cost thresholds, not just
`task_kind == "scan"`.

### Representative code patterns
```python
# src/hamilton_pipeline/execution_manager.py
class PlanExecutionManager(executors.ExecutionManager):
    def __init__(..., cost_threshold: float | None = None) -> None:
        self._cost_threshold = cost_threshold

    def get_executor_for_task(self, task: executors.TaskImplementation) -> executors.TaskExecutor:
        tags = _task_tags(task)
        if _is_scan_task(tags) or _is_high_cost(tags, threshold=self._cost_threshold):
            return self._remote_executor
        return self._local_executor
```

### Target files to modify
- `src/hamilton_pipeline/execution_manager.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/task_module_builder.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add cost threshold routing for view tasks.
- [x] Make executor routing configurable via `ExecutorConfig`.
- [x] Ensure routing decisions are logged in diagnostics.

---

## Scope 8 — Parallelize Scan Units with Dynamic DAG (Parallelizable/Collect)

### Objective
Expose per‑scan‑unit parallelism using Hamilton dynamic DAG features so scan tasks scale across
executors without bespoke orchestration logic.

### Representative code patterns
```python
# src/hamilton_pipeline/modules/inputs.py
def scan_units(plan_scan_units: tuple[ScanUnit, ...]) -> Parallelizable[ScanUnit]:
    for unit in plan_scan_units:
        yield unit

def scan_results(scan_units: Parallelizable[ScanUnit]) -> Parallelizable[TableLike]:
    return execute_scan(scan_units)

def scan_outputs(scan_results: Collect[TableLike]) -> tuple[TableLike, ...]:
    return tuple(scan_results)
```

### Target files to modify
- `src/hamilton_pipeline/modules/task_execution.py`
- `src/hamilton_pipeline/driver_factory.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Introduce Parallelizable/Collect scan‑unit nodes.
- [x] Enable dynamic execution only when `execution_mode` is parallel.
- [x] Validate determinism of output ordering.

---

## Scope 9 — Scheduling Artifacts in Run Manifest v2 (structured linking)

### Objective
Keep the run manifest lean while linking to schedule/validation artifacts by ID using **structured
fields or msgspec bytes** (no JSON columns).

### Representative code patterns
```python
# src/hamilton_pipeline/modules/outputs.py
artifact_ids = {
    "run_manifest": manifest_id,
    "plan_schedule": schedule_artifact_id,
    "plan_validation": validation_artifact_id,
}
return RunManifestV2(..., artifact_ids=artifact_ids)
```

### Target files to modify
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/modules/execution_plan.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `src/datafusion_engine/schema_registry.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add manifest links to schedule/validation artifacts in `RunManifestV2`.
- [x] Ensure artifact IDs are deterministic and stable.
- [x] Cut over manifest to v2 only and remove any v1 JSON columns/registrations.

---

## Scope 10 — Tests + Documentation Alignment

### Objective
Add golden tests for schedule/validation artifacts and update architecture docs for the new
execution mode and orchestration behavior.

### Representative code patterns
```python
# tests/msgspec_contract/test_contract_schedule.py
payload = PlanScheduleEnvelope(payload=sample_schedule())
assert_json_snapshot(payload)
assert_msgpack_snapshot(payload)
```

### Target files to modify
- `tests/msgspec_contract/test_contract_*.py`
- `tests/unit/test_plan_bundle_determinism_audit.py`
- `docs/architecture/part_vi_cpg_build_and_orchestration.md`
- `docs/architecture/ARCHITECTURE.md`

### Modules to delete
- None.

### Implementation checklist
- [x] Add new msgspec contract tests for schedule/validation artifacts.
- [x] Update determinism audit tests to include schedule artifact hashing.
- [x] Update architecture docs to reflect the new execution modes and scheduling pipeline.

---

## Scope 11 — Deferred Deletions (post‑migration cleanup)

### Objective
Identify legacy paths that should be removed only after all prior scopes are complete and verified.

### Delete candidates (defer until all scopes complete)
- Legacy reliance on `task_graph_diagnostics_v2` dict payloads once the
  msgspec schedule/validation artifacts are canonical.
- Any ad‑hoc schedule ordering code paths that ignore cost/slack once cost‑aware scheduling is live.
- Any duplicated diagnostics fields in run manifest once artifact linking is stable.

### Target files to modify
- `src/hamilton_pipeline/lifecycle.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/relspec/rustworkx_schedule.py`

### Implementation checklist
- [x] Audit for redundant schedule/diagnostic payloads.
- [x] Remove legacy paths and update tests/goldens accordingly.
- [x] Validate artifact registry and schema exports remain complete.

---

## Acceptance Gates
- All modified files pass **ruff**, **pyright**, and **pyrefly** (zero errors).
- New msgspec artifacts have JSON/MsgPack/schema/error goldens.
- Dynamic execution is controlled via first‑class API flags, not implicit config.
- Scheduling diagnostics and validation are emitted as msgspec bytes/structured columns and linked
  in the manifest v2 (no JSON payload columns).
