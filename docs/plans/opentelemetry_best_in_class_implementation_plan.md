# OpenTelemetry Best-in-Class Implementation Plan (Design Phase, Breaking Changes OK)

Date: 2026-01-29  
Owner: Codex (design phase)  
Status: in progress (code scopes complete; documentation/cleanup remaining)
Validated: 2026-01-29 (codebase cross-check; docs/cleanup pending)  

## Purpose
Deliver a best-in-class OpenTelemetry (OTel) architecture across CodeAnatomy. The target state provides end-to-end traces, metrics, and logs with consistent resource identity, strict semantic conventions, deterministic context propagation, and a clean separation between domain observability (src/obs) and telemetry transport (OTel SDK + Collector). The plan assumes breaking changes are acceptable.

## Design Principles
- **Single source of truth for telemetry**: one bootstrap layer configures providers and propagators.
- **Domain semantics first**: existing obs artifacts define what to measure; OTel defines how to ship it.
- **Stable identities**: fixed instrumentation scopes, stable resource attributes, controlled cardinality.
- **Collector-first**: export via OTLP to a Collector for sampling, redaction, and routing.
- **Testable by default**: in-memory exporters and golden contract tests for span/metric/log shape.

---

## Scope 1 — OTel bootstrap layer (providers, resources, propagators)

### Objective
Create a unified OTel bootstrap package that builds Resource, TracerProvider, MeterProvider, and LoggerProvider from env plus explicit overrides. This becomes the single entry point for telemetry initialization in all processes.

### Representative code pattern
```python
from obs.otel.bootstrap import configure_otel

providers = configure_otel(
    service_name="codeanatomy",
    service_version="3.0.0",
    environment="staging",
    resource_overrides={"codeanatomy.repo_hash": repo_hash},
)
providers.activate_global()
```

### Target files to modify
- `src/obs/otel/bootstrap.py` (new)
- `src/obs/otel/resources.py` (new)
- `src/obs/otel/config.py` (new)
- `src/obs/otel/__init__.py` (new)
- `src/engine/runtime.py`
- `src/engine/session_factory.py`
- `src/engine/materialize_pipeline.py`
- `src/graph/product_build.py`
- `docs/architecture/ARCHITECTURE.md`

### Modules to delete
- None.

### Implementation checklist
- [x] Create an OTel bootstrap module that wires TracerProvider, MeterProvider, LoggerProvider.
- [x] Centralize Resource construction with stable attributes and environment merge rules.
- [x] Standardize propagators and log correlation defaults.
- [x] Provide a test-mode config (in-memory exporters, simple span processor).
- [x] Update entry points to call the bootstrap once per process.
- [x] Bootstrap wired in runtime/session factory/materialize pipeline/product build/extract worker init.

---

## Scope 2 — Resource identity and instrumentation scopes

### Objective
Define stable, canonical instrumentation scope names and resource attributes to remove name drift and improve longitudinal telemetry consistency.

### Representative code pattern
```python
SCOPE_NAME = "codeanatomy.datafusion"
tracer = trace.get_tracer(SCOPE_NAME, instrumenting_library_version=APP_VERSION)
```

### Target files to modify
- `src/obs/otel/scopes.py` (new)
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/hamilton_pipeline/execution_manager.py`
- `src/extract/*`
- `src/normalize/*`
- `src/cpg/*`

### Modules to delete
- None.

### Implementation checklist
- [x] Define canonical scope names for each subsystem (extract, normalize, planning, datafusion, hamilton, cpg).
- [x] Attach scope names to all tracers/meters/loggers (no `__name__`).
- [x] Normalize resource attributes (service.name, service.version, environment, repo hash, plan signature).

---

## Scope 3 — Trace topology (pipeline, stage, task, DataFusion spans)

### Objective
Establish a consistent trace graph for every pipeline run: a root span, stage spans, task spans (linked), and DataFusion planning/execution spans.

### Representative code pattern
```python
with tracer.start_as_current_span("graph_product.build") as root:
    root.set_attribute("plan_signature", plan_signature)
    with tracer.start_as_current_span("extract.ast"):
        extract_ast(...)

with tracer.start_as_current_span("hamilton.task", links=[Link(root.get_span_context())]):
    run_task(...)
```

### Target files to modify
- `src/graph/product_build.py`
- `src/extract/*`
- `src/normalize/*`
- `src/relspec/*`
- `src/datafusion_engine/*`
- `src/hamilton_pipeline/*`
- `src/engine/materialize_pipeline.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Root span for every pipeline run.
- [x] Stage spans for extraction, normalization, planning, scheduling, and CPG build.
- [x] Task spans for Hamilton tasks (span links to root span).
- [x] DataFusion plan/execute spans with deterministic attributes.
- [x] Span events for diagnostics payloads (small, structured).

---

## Scope 4 — Metrics mapping from obs signals

### Objective
Map existing `src/obs/metrics.py` and scan telemetry outputs into OTel metrics with controlled cardinality and curated Views.

### Representative code pattern
```python
duration = meter.create_histogram("codeanatomy.stage.duration", unit="s")

with timed_span("extract.ast") as elapsed_s:
    duration.record(elapsed_s, {"stage": "extract", "status": "ok"})
```

### Target files to modify
- `src/obs/metrics.py`
- `src/obs/otel/metrics.py` (new)
- `src/obs/scan_telemetry.py`
- `src/datafusion_engine/plan_artifact_store.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Define a stable metric catalog for stages, artifacts, errors, and dataset stats.
- [x] Add Views to drop high-cardinality labels and set histogram buckets.
- [x] Record metrics at core stage boundaries and DataFusion execution boundaries.
- [x] Convert scan telemetry into gauge metrics (row groups, fragment counts).

---

## Scope 5 — Logs pipeline and diagnostics correlation

### Objective
Route diagnostics artifacts and events to OTel logs with trace correlation, while preserving in-memory snapshots for tests.

### Representative code pattern
```python
logger = get_logger("codeanatomy.diagnostics")
logger.emit("artifact", attributes={"artifact.kind": kind, "payload": payload})
```

### Target files to modify
- `src/obs/diagnostics.py`
- `src/obs/otel/logs.py` (new)
- `src/engine/session.py`
- `src/hamilton_pipeline/lifecycle.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add OTel log exporter wiring (optional but best-in-class).
- [x] Correlate logs with trace/span ids (log correlation enabled).
- [x] Route diagnostics artifacts through OTel logger in non-test contexts.
- [x] Keep DiagnosticsCollector for deterministic tests and local snapshots.

---

## Scope 6 — Sampling, limits, and privacy enforcement

### Objective
Apply consistent head sampling, span/attribute limits, and PII redaction policies through Collector-first patterns.

### Representative code pattern
```python
sampler = ParentBased(TraceIdRatioBased(0.1))
provider = TracerProvider(resource=resource, sampler=sampler)
```

### Target files to modify
- `src/obs/otel/config.py` (new)
- `docs/architecture/observability.md` (new)
- `docs/architecture/ARCHITECTURE.md`
- `tools/otel/collector.yaml` (new)

### Modules to delete
- None.

### Implementation checklist
- [x] Configure parent-based head sampling in the bootstrap.
- [x] Enforce attribute/span/log limits via env vars (SpanLimits + attribute normalization).
- [ ] Document limit env vars and defaults. (doc deferred)
- [x] Provide a Collector config with tail sampling, redaction, and routing.
- [x] Implement attribute redaction defaults (`CODEANATOMY_OTEL_REDACT_KEYS`).
- [ ] Adopt a privacy policy for attributes and baggage usage. (doc deferred)

---

## Scope 7 — Auto-instrumentation boundaries and manual instrumentation policy

### Objective
Combine auto-instrumentation for commodity libraries with manual instrumentation for domain-specific stages to ensure correctness and low overhead.

### Representative code pattern
```bash
OTEL_PYTHON_DISABLED_INSTRUMENTATIONS="urllib,urllib3" opentelemetry-instrument python app.py
```

### Target files to modify
- `docs/architecture/observability.md` (new)
- `scripts/run_pipeline.sh` (or equivalent entry script)
- `src/engine/session_factory.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Define which libraries rely on auto-instrumentation vs manual instrumentation.
- [x] Document OTEL_PYTHON_* knobs for excluded URLs and logging correlation.
- [x] Ensure manual instrumentation sets attributes at span creation (for sampling).

---

## Scope 8 — Concurrency, forks, and context propagation

### Objective
Ensure telemetry correctness in multiprocessing (extract stage), thread pools, and async execution via explicit init hooks and context boundaries.

### Representative code pattern
```python
from opentelemetry.instrumentation.auto_instrumentation import initialize

def worker_init() -> None:
    initialize()
```

### Target files to modify
- `src/extract/parallel.py`
- `src/hamilton_pipeline/execution_manager.py`
- `src/datafusion_engine/runtime.py`
- `docs/architecture/observability.md` (new)

### Modules to delete
- None.

### Implementation checklist
- [x] Initialize OTel inside worker processes post-fork.
- [x] Ensure context propagation for thread pools and async tasks.
- [x] Add explicit attach/detach boundaries where required.

---

## Scope 9 — Testing harness and observability contracts

### Objective
Establish contract tests for spans, metrics, and logs (names, attributes, cardinality) with in-memory exporters and golden snapshots.

### Representative code pattern
```python
exporter = InMemorySpanExporter()
provider.add_span_processor(SimpleSpanProcessor(exporter))
assert span.name == "extract.ast"
```

### Target files to modify
- `tests/obs/test_otel_traces_contract.py` (new)
- `tests/obs/test_otel_metrics_contract.py` (new)
- `tests/obs/test_otel_logs_contract.py` (new)
- `tests/obs/_support/otel_harness.py` (new)

### Modules to delete
- None.

### Implementation checklist
- [x] Add in-memory exporters for traces/metrics/logs in tests.
- [x] Snapshot contract goldens for span names and key attributes.
- [x] Validate metric catalog and label cardinality rules.
- [x] Validate log correlation fields for diagnostics events.

---

## Scope 10 — Documentation and operator runbooks

### Objective
Provide operational documentation for configuring and running OTel in dev, CI, and production.

### Representative code pattern
```bash
export OTEL_SERVICE_NAME="codeanatomy"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317"
```

### Target files to modify
- `docs/architecture/observability.md` (new)
- `docs/architecture/ARCHITECTURE.md`
- `docs/python_library_reference/opentelemetry.md` (reference links)

### Modules to delete
- None.

### Implementation checklist
- [x] Document bootstrap usage and env var matrix.
- [x] Provide Collector configuration examples.
- [x] Provide dev/test/prod profiles with different sampling and exporters.
- [ ] Update reference links in `docs/python_library_reference/opentelemetry.md`.
- [ ] Align `docs/architecture/observability.md` metric names/units with seconds-based catalog.

---

## Scope 11 — Deferred deletions and cleanup (post-scope)

### Objective
Identify temporary compatibility paths and legacy diagnostics flows to delete after all scopes are complete.

### Candidate deletions (defer until all scopes land)
- Legacy, non-OTel diagnostics sinks used only for production routing (retain for tests).
- Ad-hoc timing/metric counters replaced by OTel metrics in the obs layer.
- Redundant custom log correlation utilities (when OTel log correlation is enabled).

### Implementation checklist
- [ ] Audit diagnostics sinks and remove unused production-only branches.
- [x] Remove duplicated metric collection once OTel meters are in place.
- [ ] Confirm no tests rely on deprecated sinks before deletion.

---

## Acceptance Gates (pending)
- All pipeline stages emit traces with stable span names and required attributes.
- Metrics catalog matches documented names and has bounded cardinality.
- Logs are correlated to traces (trace_id/span_id present).
- Collector-first export is the default path for all signals.
- Contract tests cover spans, metrics, and logs and are deterministic.
