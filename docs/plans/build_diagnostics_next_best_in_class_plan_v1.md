# Build Diagnostics Next Best-in-Class Plan (v1)

## Objective
Extend the completed build diagnostics foundation with deeper provider-mode visibility, Delta protocol health checks, DataFusion plan-phase telemetry, and standardized diagnostic taxonomy. The goal is to pinpoint errors, integration gaps, and performance bottlenecks in a single run without relying on fallbacks.

---

## Scope Item 1 — Delta ↔ DataFusion Provider Mode Diagnostics
**Goal:** Make it explicit whether each dataset is registered via Delta TableProvider or a fallback (e.g., Arrow Dataset), and surface the consequences in diagnostics.

**Representative code snippet**
```python
# src/datafusion_engine/dataset/registration.py
record_artifact(
    profile,
    "dataset_provider_mode_v1",
    {
        "dataset": name,
        "provider_mode": provider_mode,  # delta_table_provider | pyarrow_dataset | other
        "provider_class": provider_class,
        "run_id": get_run_id(),
    },
)
```

**Target files**
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/session/runtime.py`
- `src/obs/diagnostics_report.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Emit provider mode diagnostics for every dataset registration.
- [x] Record provider class name + feature gate signals (e.g., FFI TableProvider).
- [x] Summarize provider modes in `run_diagnostics_report.md`.

---

## Scope Item 2 — Delta Log Health + Protocol Compatibility
**Goal:** Diagnose Delta readiness at the protocol level (log presence, checkpoints, minReader/minWriter, table features) and surface compatibility risks.

**Representative code snippet**
```python
# src/datafusion_engine/dataset/registration.py
record_artifact(
    profile,
    "delta_log_health_v1",
    {
        "dataset": name,
        "delta_log_present": delta_log_present,
        "last_checkpoint_version": checkpoint_version,
        "min_reader_version": min_reader_version,
        "min_writer_version": min_writer_version,
        "table_features": sorted(table_features),
        "protocol_compatible": protocol_compatible,
    },
)
```

**Target files**
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/delta/protocol.py`
- `src/obs/diagnostics_report.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Capture `_delta_log` presence and checkpoint recency.
- [x] Emit minReader/minWriter + table features per dataset.
- [x] Flag protocol incompatibilities with explicit diagnostics.

---

## Scope Item 3 — DataFusion Plan Phase Telemetry
**Goal:** Emit diagnostics across logical → optimized → physical plan phases, with stable fingerprints for each phase and duration metrics.

**Representative code snippet**
```python
# src/datafusion_engine/plan/diagnostics.py
record_artifact(
    runtime_profile,
    "datafusion_plan_phase_v1",
    {
        "plan_hash": plan_hash,
        "phase": phase,  # logical | optimized | physical
        "duration_ms": duration_ms,
        "trace_id": trace_id,
        "span_id": span_id,
    },
)
```

**Target files**
- `src/datafusion_engine/plan/bundle.py`
- `src/datafusion_engine/plan/diagnostics.py`
- `src/obs/diagnostics_report.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Record plan hashes for each phase.
- [x] Emit timing for each phase with trace/span IDs.
- [x] Summarize phase durations in `run_diagnostics_report.md`.

---

## Scope Item 4 — Scan Unit Pruning + File-Level Visibility
**Goal:** Surface scan pruning metrics and file counts for each dataset, so performance bottlenecks are explainable.

**Representative code snippet**
```python
# src/datafusion_engine/lineage/scan.py
record_artifact(
    runtime_profile,
    "scan_unit_pruning_v1",
    {
        "dataset": dataset_name,
        "total_files": total_files,
        "candidate_files": candidate_file_count,
        "pruned_files": pruned_file_count,
        "protocol_compatible": protocol_compatible,
    },
)
```

**Target files**
- `src/datafusion_engine/lineage/scan.py`
- `src/obs/diagnostics_report.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Emit pruning counts for every planned scan unit.
- [x] Attach dataset name + protocol compatibility to scan diagnostics.
- [x] Summarize top datasets with highest pruning ratios.

---

## Scope Item 5 — Diagnostics Taxonomy (Severity + Category)
**Goal:** Standardize diagnostics records with severity/category to enable consistent triage and report aggregation.

**Representative code snippet**
```python
# src/obs/otel/logs.py
emit_diagnostics_event(
    "delta_log_health_v1",
    payload={
        "diagnostic.severity": "warn",
        "diagnostic.category": "delta_protocol",
        **payload,
    },
    event_kind="artifact",
)
```

**Target files**
- `src/obs/otel/logs.py`
- `src/obs/diagnostics_report.py`
- `docs/architecture/diagnostics_policy.md`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add `diagnostic.severity` and `diagnostic.category` for all new events.
- [x] Update diagnostics policy to document severity meanings.
- [x] Aggregate warnings/errors by category in report output.

---

## Scope Item 6 — Cyclopts Telemetry Config Hook
**Goal:** Ensure telemetry config is resolved during CLI parse, so parse/validation errors still record full telemetry context.

**Representative code snippet**
```python
# src/cli/app.py
app.config.append(
    lambda apps, commands, arguments: apply_telemetry_config(arguments)
)
```

**Target files**
- `src/cli/app.py`
- `src/obs/otel/bootstrap.py`
- `src/cli/telemetry.py`

**Deprecate/Delete after completion**
- None.

**Implementation checklist**
- [x] Add `App.config` hook to hydrate telemetry config pre-validation.
- [x] Record effective OTel config snapshot for each run.
- [x] Confirm parse errors still emit structured diagnostics.

---

## Cross-Cutting Acceptance Gates
- Run diagnostics report includes provider-mode summary, Delta log health, and plan-phase timings.
- Any fallback provider mode is explicitly diagnosed as a warning with category `datafusion_provider`.
- Delta protocol incompatibilities are captured with `diagnostic.severity=error`.
- All diagnostics events include severity + category.
- CLI parse errors still emit a telemetry event with full configuration context.

## Status (2026-02-03)
- Implementation scope completed in code.
- Remaining work is runtime verification: run a full build and confirm the acceptance gates by inspecting the generated run diagnostics report.
