# Build Diagnostics Best-in-Class Plan (v1)

## Objective
Upgrade build diagnostics to a best-in-class, end-to-end observability surface that reliably pinpoints failures, stalls, and data-quality gaps while aligning with the semantic-IR architecture. This plan captures agreed recommendations as scope items with representative code snippets, target files, deprecations, and implementation checklists.

---

## Status Summary (as of 2026-02-03)
- **Completed:** Scope Items 1–9
- **Partially completed:** None
- **Not started:** None

## Scope Item 1 — CLI Telemetry Runner (Cyclopts + OTel)
**Goal:** Instrument the full Cyclopts parse → dispatch → exit lifecycle with a single root span and consistent exit/error telemetry.
**Status:** **Completed** (implemented via `src/cli/telemetry.py` + `src/cli/app.py`; CLI args are not captured—only token count is recorded).

**Representative code snippet**
```python
# src/cli/runner.py
@dataclass(frozen=True)
class RunContext:
    invocation_id: str
    command_chain: tuple[str, ...]
    start_ns: int
    logger: logging.Logger


def run_with_telemetry(app: App, argv: list[str] | None = None) -> None:
    start_ns = time.time_ns()
    invocation_id = uuid7_str()
    logger = logging.getLogger("cli")
    with root_span(
        "cli.invocation",
        attributes={
            "cli.invocation_id": invocation_id,
            "cli.start_ns": start_ns,
        },
        scope_name=SCOPE_CLI,
    ):
        command, bound, ignored = app.parse_args(argv, exit_on_error=False, print_error=True)
        ctx = RunContext(
            invocation_id=invocation_id,
            command_chain=_command_chain(command),
            start_ns=start_ns,
            logger=logger,
        )
        _inject_run_context(bound, ignored, ctx)
        result = command(*bound.args, **bound.kwargs)
        _emit_cli_finish(result, ctx)
```

**Target files**
- `src/cli/telemetry.py`
- `src/cli/app.py`
- `src/cli/commands/build.py`
- `src/cli/commands/config.py`
- `src/cli/commands/plan.py`
- `src/obs/otel/logging.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add a single telemetry runner that wraps Cyclopts parse/dispatch/exit.
- [x] Inject `RunContext` via `parse=False` parameters for all commands.
- [x] Emit `cli.exit_code`, `cli.duration_ms`, and `cli.error.type` attributes (duration is represented via parse/exec timings).
- [x] Sanitize/allowlist CLI args before attaching to telemetry (args not captured; token count only).

---

## Scope Item 2 — Hamilton UI Tracker Integration (Optional but First-Class)
**Goal:** Add Hamilton UI/DAGWorks tracker adapter wiring for DAG versioning, node timing, and run comparisons.
**Status:** **Completed** (adapter wiring and tagging exist in `src/hamilton_pipeline/driver_factory.py`).

**Representative code snippet**
```python
# src/hamilton_pipeline/driver_factory.py
tracker = adapters.HamiltonTracker(
    project_id=int(os.environ["HAMILTON_PROJECT_ID"]),
    username=os.environ["HAMILTON_USERNAME"],
    dag_name=f"codeanatomy::semantic_ir_v1::{repo_hash}",
    tags={
        "environment": env_value("CODEANATOMY_ENVIRONMENT") or "dev",
        "semantic_version": "v1",
        "repo_hash": repo_hash,
    },
    hamilton_api_url=os.environ.get("HAMILTON_API_URL"),
    hamilton_ui_url=os.environ.get("HAMILTON_UI_URL"),
)
```

**Target files**
- `src/hamilton_pipeline/driver_factory.py`
- `src/graph/product_build.py`
- `src/obs/otel/config.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add optional Hamilton tracker adapter wiring behind config/env.
- [x] Define stable `dag_name` semantics aligned with semantic-IR versions.
- [x] Attach tags for repo hash, environment, semantic version.

---

## Scope Item 3 — Full OpenTelemetry Configuration Surface
**Goal:** Expose OTel advanced configuration (samplers, batching, exporters, limits) and support config files.
**Status:** **Completed** (config file support + exporter/limits added; deployment environment attached to resource).

**Representative code snippet**
```python
# src/obs/otel/config.py
@dataclass(frozen=True)
class OtelConfig:
    sampler: str | None
    sampler_arg: str | None
    bsp_schedule_delay_ms: int | None
    bsp_export_timeout_ms: int | None
    bsp_max_queue_size: int | None
    bsp_max_export_batch_size: int | None
    attribute_count_limit: int | None
    attribute_value_length_limit: int | None
```

**Target files**
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/resources.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add support for `OTEL_EXPERIMENTAL_CONFIG_FILE` if present.
- [x] Plumb BSP/BLRP and metric reader settings from env/config.
- [x] Apply attribute length/count limits to prevent cardinality blowups.
- [x] Ensure `service.name`, `service.version`, `deployment.environment` are always set.

---

## Scope Item 4 — Dataset Readiness + Registration Diagnostics
**Goal:** Emit explicit readiness diagnostics before view planning/execution to avoid silent stalls.
**Status:** **Completed** (readiness recorded in `src/hamilton_pipeline/driver_factory.py`, summarized in diagnostics report).

**Representative code snippet**
```python
# src/datafusion_engine/session/runtime.py
recorder.record_artifact(
    "dataset_readiness_v1",
    {
        "dataset": name,
        "path": str(location.path),
        "format": location.format,
        "delta_log_present": delta_log.exists(),
        "file_count": file_count,
        "reason": reason,
    },
)
```

**Target files**
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/catalog/provider.py`
- `src/obs/diagnostics_report.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add readiness checks for all configured dataset locations.
- [x] Emit diagnostics for missing/empty delta logs or missing files.
- [x] Summarize readiness issues in `run_diagnostics_report.md`.

---

## Scope Item 5 — Plan vs Execution Diff Diagnostics
**Goal:** Identify tasks expected vs executed and surface blocking datasets immediately.
**Status:** **Completed** (expected/executed/missing task counts, blockers, and plan signatures included).

**Representative code snippet**
```python
# src/obs/diagnostics_report.py
report = {
    "expected_tasks": sorted(plan.active_tasks),
    "executed_tasks": sorted(executed_tasks),
    "missing_tasks": sorted(set(plan.active_tasks) - set(executed_tasks)),
    "blocked_by": blocked_by_dataset,
}
```

**Target files**
- `src/relspec/execution_plan.py`
- `src/hamilton_pipeline/execution.py`
- `src/obs/diagnostics_report.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Record expected vs executed task sets with timestamps.
- [x] Attach blockers from missing datasets and scan units.
- [x] Include plan signature + fingerprints in diagnostics output.

---

## Scope Item 6 — DataFusion Plan + Execution Metrics
**Goal:** Capture physical plan artifacts and execution metrics (rows, bytes, partitions) with span linkage.
**Status:** **Completed** (execution metrics recorded; trace/span IDs embedded in artifacts).

**Representative code snippet**
```python
# src/datafusion_engine/plan/diagnostics.py
record_artifact(
    runtime_profile,
    "datafusion_plan_execution_v1",
    {
        "plan_hash": plan_hash,
        "rows_produced": rows,
        "bytes_scanned": bytes,
        "partitions": partitions,
    },
)
```

**Target files**
- `src/datafusion_engine/plan/diagnostics.py`
- `src/datafusion_engine/plan/execution.py`
- `src/datafusion_engine/plan/bundle.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Emit plan bundles for all view plans in debug mode.
- [x] Capture execution stats for slow or failed plans.
- [x] Link artifacts to active spans via trace/span IDs.

---

## Scope Item 7 — Trace/Log Correlation + Structured Event Schema
**Goal:** Ensure every log record includes trace/span IDs and standardized event names.
**Status:** **Completed** (trace/log correlation installed; build event schema emitted).

**Representative code snippet**
```python
# src/obs/otel/logging.py
install_trace_context_filter()
apply_trace_context_formatter(fmt=TRACE_LOG_FORMAT, force=True)
```

**Target files**
- `src/obs/otel/logging.py`
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/logs.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add trace/span IDs to all standard loggers.
- [x] Define canonical event names (build.start, build.phase.start, build.phase.end, build.failure).
- [x] Ensure OTel logs are emitted even without full tracing enabled.

---

## Scope Item 8 — Heartbeat + Stall Detection Enhancements
**Goal:** Convert heartbeat into an actionable stall detector with blocking context.
**Status:** **Completed** (heartbeat includes task counts + blockers; stall events emitted after threshold).

**Representative code snippet**
```python
# src/obs/otel/heartbeat.py
payload.update({
    "active_task_count": active_tasks,
    "pending_task_count": pending_tasks,
    "top_blockers": blockers,
})
```

**Target files**
- `src/obs/otel/heartbeat.py`
- `src/hamilton_pipeline/execution.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Include active/pending task counts in heartbeat payloads.
- [x] Emit `build_stall_v1` when idle gap exceeds threshold.
- [x] Record blockers from missing datasets or failed scans.

---

## Scope Item 9 — Telemetry Safety & Overhead Policy
**Goal:** Provide safe defaults for data capture and ensure diagnosability without leaking sensitive payloads.
**Status:** **Completed** (limits + policy doc added; list/dict truncation enforced).

**Representative code snippet**
```python
# src/obs/otel/config.py
CAPTURE_DATA_STATISTICS = env_bool("HAMILTON_CAPTURE_DATA_STATISTICS", default=False)
MAX_LIST_LENGTH = env_int("HAMILTON_MAX_LIST_LENGTH_CAPTURE", default=20)
```

**Target files**
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`
- `docs/architecture/diagnostics_policy.md` (new)

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add capture limits for lists/dicts and disable data stats in prod by default.
- [x] Gate high-cardinality attributes behind debug mode.
- [x] Document telemetry governance and recommended env vars.

---

## Cross-Cutting Acceptance Gates
- **Met:** CLI parse/dispatch/exit produces a root span and exit telemetry.
- **Met:** Hamilton UI tracking can be enabled via config and shows DAG versions + runs.
- **Met:** Run bundle includes dataset readiness diagnostics and plan/execution diffs.
- **Met:** Logs are trace‑correlated and structured across all build phases.
- **Met:** Heartbeat identifies stalls with actionable blocker context.
- **Met:** No sensitive CLI args or payloads are captured by default (args omitted entirely).
