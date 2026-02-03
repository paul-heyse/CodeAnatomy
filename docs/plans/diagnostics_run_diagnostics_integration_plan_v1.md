# Diagnostics Run Diagnostics Integration Plan v1

Status: Proposed  
Owner: Codex  
Scope: Unified diagnostics bundle + performance/hang investigation for full builds

## Objectives
- Emit a single, run-scoped diagnostics bundle that aggregates traces, metrics, and logs.
- Provide a build progress heartbeat to avoid "silent" hangs.
- Standardize span attributes across all major stages for fast diagnosis.
- Capture DataFusion plan artifacts and attach them to spans.
- Generate a post-run synthesis report highlighting slow/stalled phases.

---

## Scope Item 1 — Unified Run Diagnostics Bundle (Traces/Metrics/Logs)

### Why
Diagnostics exist but are fragmented. We need a single run-scoped bundle that can be inspected offline and correlates all signals.

### Design (best-in-class choice)
Add a diagnostics sink that writes trace/metric/log exports into the run bundle directory, keyed by run_id. Export JSON (or protobuf) snapshots alongside OTLP for downstream tools.

### Representative snippet
```python
# src/obs/diagnostics_bundle.py
def write_run_diagnostics_bundle(
    *,
    run_bundle_dir: Path,
    trace_snapshot: dict[str, object],
    metrics_snapshot: dict[str, object],
    logs_snapshot: dict[str, object],
) -> None:
    run_bundle_dir.mkdir(parents=True, exist_ok=True)
    write_json(run_bundle_dir / "otel_traces.json", trace_snapshot)
    write_json(run_bundle_dir / "otel_metrics.json", metrics_snapshot)
    write_json(run_bundle_dir / "otel_logs.json", logs_snapshot)
```

### Target files
- `src/obs/otel/bootstrap.py`
- `src/obs/diagnostics_bundle.py` (new)
- `src/graph/product_build.py`
- `src/obs/diagnostics.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add a diagnostics bundle writer that targets `run_bundle_dir`.
- Ensure trace/metric/log snapshots can be emitted on demand.
- Attach run_id + output_dir metadata to each snapshot.
- Wire bundle creation at build completion (success or failure).

---

## Scope Item 2 — Build Progress Heartbeat (Hang Detection)

### Why
Long silent periods are indistinguishable from deadlocks. A heartbeat gives visibility into "is anything happening?"

### Design
Emit a lightweight heartbeat span or log at a fixed interval (e.g., every 5s) containing stage, active tasks, and IO counters.

### Representative snippet
```python
# src/obs/otel/heartbeat.py
def emit_build_heartbeat(ctx: BuildRuntimeContext) -> None:
    with stage_span(
        "build.heartbeat",
        stage=ctx.stage,
        attributes={
            "codeanatomy.run_id": ctx.run_id,
            "codeanatomy.active_tasks": ctx.active_tasks,
            "codeanatomy.io.bytes_read": ctx.bytes_read,
            "codeanatomy.io.bytes_written": ctx.bytes_written,
        },
    ):
        pass
```

### Target files
- `src/obs/otel/heartbeat.py` (new)
- `src/graph/product_build.py`
- `src/hamilton_pipeline/executor.py` (or task scheduler module)

### Deprecations / deletions
- None.

### Implementation checklist
- Add a heartbeat loop with configurable interval.
- Include stage, active tasks, and IO stats.
- Ensure heartbeat stops on build completion or failure.

---

## Scope Item 3 — Span Hygiene + Standard Attributes

### Why
Traces are only useful if span metadata is consistent and searchable across stages.

### Design
Standardize span attributes (stage, dataset, table, row_count, bytes, plan_hash) and apply them to all critical stages.

### Representative snippet
```python
# src/obs/otel/tracing.py
def stage_span(
    name: str,
    *,
    stage: str,
    attributes: Mapping[str, object] | None = None,
) -> ContextManager[Span]:
    return start_span(
        name,
        attributes={
            "codeanatomy.stage": stage,
            **(attributes or {}),
        },
    )
```

### Target files
- `src/obs/otel/tracing.py`
- `src/semantics/ir_pipeline.py`
- `src/datafusion_engine/plan/bundle.py`
- `src/hamilton_pipeline/*`

### Deprecations / deletions
- None.

### Implementation checklist
- Define standard attribute keys in one module.
- Apply to key phases: extract, normalize, compile, plan, register, write.
- Add slow-threshold tagging (`codeanatomy.slow=true`).

---

## Scope Item 4 — DataFusion Plan Diagnostics + Artifacts

### Why
The build can be slow due to plan compilation or execution. We need plan visibility tied to spans.

### Design
Capture logical/physical plan bundles and register them as artifacts. Attach `plan_hash`, `query_id`, and `source_tables` to spans.

### Representative snippet
```python
# src/datafusion_engine/plan/diagnostics.py
bundle = build_plan_bundle(ctx, df, PlanBundleOptions(include_physical=True))
record_artifact(
    "datafusion_plan_bundle_v1",
    {"plan_hash": bundle.plan_hash, "bundle": bundle.to_dict()},
)
```

### Target files
- `src/datafusion_engine/plan/bundle.py`
- `src/datafusion_engine/plan/diagnostics.py` (new)
- `src/datafusion_engine/dataset/registration.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Emit plan bundle artifacts for slow plans or all plans in debug mode.
- Attach plan metadata to the active span.
- Link artifacts to `run_bundle_dir` with deterministic filenames.

---

## Scope Item 5 — Post-Run Diagnostics Synthesis Report

### Why
We need a single artifact summarizing slow spans, warnings, and idle time.

### Design
Generate `run_diagnostics_report.md` and `run_diagnostics_report.json` by aggregating traces/metrics/logs.

### Representative snippet
```python
# src/obs/diagnostics_report.py
def summarize_run_diagnostics(traces: TraceSnapshot, metrics: MetricSnapshot) -> Report:
    return Report(
        slow_spans=top_n_slowest(traces, n=10),
        stage_breakdown=group_by_stage(traces),
        io_summary=aggregate_io(metrics),
    )
```

### Target files
- `src/obs/diagnostics_report.py` (new)
- `src/graph/product_build.py`
- `src/obs/diagnostics_bundle.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Define report schema for slow spans + stage timing.
- Add IO summary (bytes read/written, rows).
- Write report into run bundle directory.

---

## Scope Item 6 — Log Correlation with Trace IDs

### Why
Logs need to be trace-linked to be actionable.

### Design
Inject `trace_id` and `span_id` into log records when an active span exists.

### Representative snippet
```python
# src/obs/otel/logging.py
class TraceContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = current_trace_id()
        record.span_id = current_span_id()
        return True
```

### Target files
- `src/obs/otel/logging.py`
- `src/obs/otel/bootstrap.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add logging filter with trace/span IDs.
- Update logging formatter to include IDs.
- Ensure filter is only enabled when OTel tracing is active.

---

## Cross-Cutting Acceptance Gates
- Run bundle contains `otel_traces.json`, `otel_metrics.json`, `otel_logs.json`.
- Heartbeat emitted at fixed intervals with active stage + IO counters.
- All major stages include standard span attributes.
- DataFusion plan bundle artifacts exist and are linked to spans.
- `run_diagnostics_report.md` summarizes slow spans and stage timing.
- Logs include trace_id/span_id when tracing is enabled.
