# Hamilton Telemetry Best-in-Class Implementation Plan (Design Phase, Breaking Changes OK)

Date: 2026-01-29
Owner: Codex (design phase)
Status: complete
Last reviewed: 2026-01-29

## Purpose
Define and implement a best-in-class Hamilton telemetry surface that fully activates the Hamilton UI (catalog + lineage + run telemetry) and integrates it with our existing OpenTelemetry and diagnostics infrastructure. This plan prioritizes first-class artifact capture, run/version semantics, governance controls, and cross-signal correlation.

## Design Principles
- **UI-first telemetry**: the Hamilton UI becomes the canonical surface for run telemetry, DAG versioning, and the artifact catalog.
- **Governed capture**: data statistics capture is explicit, bounded, and environment-controlled.
- **Deterministic versioning**: driver build = DAG version check; run execution = run record.
- **Cross-signal correlation**: Hamilton run telemetry is linkable to OpenTelemetry traces/logs/metrics.
- **Breaking changes OK**: prefer clean and explicit interfaces over backward compatibility.

---

## Scope 1 — Hamilton materializers for artifact catalog + data summaries

### Objective
Enable the Hamilton UI artifact catalog and data summaries by registering materializers for DataFusion outputs, param tables, and other pipeline artifacts. Ensure that materialized outputs are discoverable with stable names and metadata tags.

### Representative code pattern
```python
from hamilton.io.materialization import to

materializers = [
    to.dataframe(  # or a custom DataFusion/Arrow materializer wrapper
        id="codeanatomy.datafusion.table",
        output_name="semantic.v_function_risk_v1",
        metadata={
            "materialization": "datafusion_table",
            "materialized_name": "semantic.v_function_risk_v1",
        },
    ),
]

builder = builder.with_materializers(*materializers)
```

### Target files to modify
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/modules/params.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/semantic_registry.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Implement materializers for DataFusion outputs (tables/views) with stable external names.
- [x] Materialize param tables when enabled, with explicit materialized metadata.
- [x] Ensure materialized outputs carry semantic tags (materialization + materialized_name).
- [x] Emit artifact metadata in the Hamilton UI (catalog entries align with semantic registry).

---

## Scope 2 — Telemetry capture governance (Hamilton SDK constants)

### Objective
Enforce safe capture defaults via Hamilton SDK constants and environment overrides. Disable statistics in production by default and clamp list/dict capture sizes.

### Representative code pattern
```python
from hamilton_sdk.tracking import constants as sdk_constants

sdk_constants.CAPTURE_DATA_STATISTICS = False
sdk_constants.MAX_LIST_LENGTH_CAPTURE = 20
sdk_constants.MAX_DICT_LENGTH_CAPTURE = 50
```

### Target files to modify
- `src/hamilton_pipeline/driver_factory.py`
- `src/engine/runtime_profile.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add runtime-profile driven defaults for SDK capture constants.
- [x] Allow env overrides for dev/CI (HAMILTON_CAPTURE_DATA_STATISTICS, etc.).
- [x] Include capture policy in driver build diagnostics metadata.

---

## Scope 3 — Run-level tags + deterministic versioning semantics

### Objective
Clarify the boundary between DAG versioning and run execution by adding run-level metadata to tracker tags and enforcing a stable `dag_name` convention.

### Representative code pattern
```python
tracker = HamiltonTracker(
    project_id=..., username=..., dag_name="codeintel::semantic_v1",
    tags={
        "environment": "prod",
        "team": "platform",
        "plan_signature": plan.plan_signature,
        "runtime_profile": runtime_profile,
        "session_runtime_hash": session_runtime_hash,
        "run_id": run_id,
        "commit": commit,
        "repo": repo,
    },
)
```

### Target files to modify
- `src/hamilton_pipeline/driver_factory.py`
- `src/engine/runtime_profile.py`
- `src/hamilton_pipeline/execution.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Enforce a stable `dag_name` for versioning (no commit in dag_name).
- [x] Add per-run tags (run_id, commit, repo, session_runtime_hash).
- [x] Ensure run tags are attached at execution time, not only build time.

---

## Scope 4 — OTel and Hamilton UI correlation

### Objective
Make Hamilton UI runs linkable to OTel traces/logs and vice versa by stamping trace/span IDs into Hamilton tracker tags and Hamilton metadata into OTel spans.

### Representative code pattern
```python
# OTel -> Hamilton
trace_id = format(span.get_span_context().trace_id, "032x")
tracker_tags["otel.trace_id"] = trace_id

# Hamilton -> OTel
span.set_attribute("hamilton.run_id", run_id)
span.set_attribute("hamilton.dag_name", dag_name)
```

### Target files to modify
- `src/obs/otel/hamilton.py`
- `src/hamilton_pipeline/execution.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/obs/otel/attributes.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add OTel trace/span IDs to Hamilton tracker tags on run start.
- [x] Add Hamilton run metadata to OTel spans/logs as attributes.
- [x] Document correlation fields for UI and tracing backends.

---

## Scope 5 — Tracker lifecycle and flush semantics

### Objective
Ensure tracker data is flushed at process shutdown and after run completion (sync + async) to prevent telemetry loss.

### Representative code pattern
```python
try:
    result = driver.execute(...)
finally:
    tracker.flush()
```

### Target files to modify
- `src/hamilton_pipeline/execution.py`
- `src/hamilton_pipeline/driver_factory.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add explicit tracker flush/close in sync execution paths.
- [x] Add explicit tracker flush/close in async execution paths.
- [x] Ensure failures still flush telemetry.

---

## Scope 6 — Telemetry profile policy (dev/CI/prod)

### Objective
Define explicit telemetry profiles that toggle Hamilton tracker enablement, capture volume, and endpoints by environment. Make it deterministic and documented.

### Representative code pattern
```python
if profile == "prod":
    enable_hamilton_tracker = True
    capture_stats = False
    max_list = 20
elif profile == "dev":
    enable_hamilton_tracker = True
    capture_stats = True
    max_list = 200
```

### Target files to modify
- `src/engine/runtime_profile.py`
- `src/hamilton_pipeline/driver_factory.py`
- `docs/architecture/observability.md`

### Modules to delete
- None.

### Implementation checklist
- [x] Add explicit telemetry profile selection and defaults.
- [x] Wire profiles into tracker + capture constants.
- [x] Document profile behavior and environment knobs.

---

## Scope 7 — Deferred deletions and cleanup (post-scope)

### Objective
Remove any redundant telemetry paths or legacy adapter plumbing once all scopes are implemented and validated.

### Candidate deletions (defer until all scopes land)
- Redundant artifact recording paths that duplicate materializer outputs.
- Legacy tracker tag scaffolding that is superseded by run-level tagging.
- Temporary compatibility helpers once production profiles are stable.

### Implementation checklist
- [x] Confirm materializers fully cover catalog + artifact capture.
- [x] Remove deprecated telemetry helper functions. (No legacy helpers remained.)
- [x] Verify no duplicate run telemetry paths remain.

---

## Acceptance Gates
- Hamilton UI shows DAG versions, run telemetry, and artifact catalog entries for all materialized outputs.
- Telemetry capture is governed by explicit profile and SDK constants.
- Run-level tags include semantic identifiers, plan signatures, and runtime hashes.
- OTel traces/logs correlate to Hamilton runs via shared IDs.
- Tracker flush is guaranteed on run completion and failure.
