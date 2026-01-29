# OpenTelemetry Additional Best-in-Class Implementation Plan

> **Goal**: Apply all recommended follow-on OpenTelemetry improvements for CodeAnatomy to reach a best-in-class, spec-aligned design target (design phase, breaking changes allowed).

---

## Scope 1 — Propagators must respect `OTEL_PROPAGATORS`

**Intent**: Replace the hard-coded propagator set with a spec-compliant env-driven configuration so cross-system interop (B3/Jaeger/XRay) works without code changes.

**Representative pattern**:

```python
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.propagators.textmap import get_global_textmap

# Prefer: use OpenTelemetry's configured global propagator rather than hard-coding
configured = get_global_textmap()
if configured is not None:
    set_global_textmap(configured)
else:
    set_global_textmap(CompositePropagator([...]))
```

**Target files**:
- `src/obs/otel/bootstrap.py`

**Deletions**:
- Remove hard-coded `_set_propagators()` default that ignores `OTEL_PROPAGATORS` (or reduce to fallback-only behavior).

**Checklist**:
- [ ] Implement env-driven propagator resolution
- [ ] Preserve TraceContext+Baggage only as fallback
- [ ] Verify no downstream modules depend on hard-coded propagators

---

## Scope 2 — Batch processor configuration via spec env vars

**Intent**: Wire `OTEL_BSP_*`, `OTEL_BLRP_*`, and `OTEL_METRIC_EXPORT_*` to processor/reader constructors to enable runtime tuning.

**Representative pattern**:

```python
BatchSpanProcessor(
    exporter,
    max_queue_size=_env_int("OTEL_BSP_MAX_QUEUE_SIZE", default=2048),
    schedule_delay_millis=_env_int("OTEL_BSP_SCHEDULE_DELAY", default=5000),
    max_export_batch_size=_env_int("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", default=512),
    export_timeout_millis=_env_int("OTEL_BSP_EXPORT_TIMEOUT", default=30000),
)
```

**Target files**:
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Add env parsing for BSP/BLRP settings
- [ ] Validate queue/batch invariants (`batch <= queue`)
- [ ] Pass configured values into BatchSpanProcessor / BatchLogRecordProcessor
- [ ] Keep default values aligned with spec defaults

---

## Scope 3 — Resource detectors + `service.instance.id`

**Intent**: Enrich resources with runtime/container/cloud metadata using resource detectors; include `service.instance.id` for instance uniqueness.

**Representative pattern**:

```python
from opentelemetry.sdk.resources import Resource, get_aggregated_resources

detectors = ["process", "os", "container", "k8s"]
resource = get_aggregated_resources(detectors, initial_resource=base_resource)
resource = resource.merge(Resource.create({"service.instance.id": instance_id}))
```

**Target files**:
- `src/obs/otel/resources.py`
- `src/obs/otel/bootstrap.py`
- (new) `src/obs/otel/resource_detectors.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Add configurable detector list (`OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`)
- [ ] Add `service.instance.id` (stable per process)
- [ ] Preserve existing resource overrides (from options)

---

## Scope 4 — Deterministic `run_id` across spans/logs/metrics

**Intent**: Make `codeanatomy.run_id` universally available via contextvars + processors to unify traces, logs, and metrics for a run.

**Representative pattern**:

```python
# obs/otel/run_context.py
RUN_ID: ContextVar[str | None] = ContextVar("codeanatomy.run_id", default=None)

class RunIdSpanProcessor(SpanProcessor):
    def on_start(self, span: Span, parent_context=None) -> None:
        run_id = RUN_ID.get()
        if run_id:
            span.set_attribute("codeanatomy.run_id", run_id)
```

**Target files**:
- `src/obs/otel/run_context.py` (new)
- `src/obs/otel/processors.py` (new)
- `src/obs/otel/bootstrap.py`
- `src/graph/product_build.py`
- `src/engine/runtime.py` / `src/engine/materialize_pipeline.py`

**Deletions**:
- Remove ad-hoc `codeanatomy.run_id` handling on a per-span basis where redundant.

**Checklist**:
- [ ] Add run contextvar (set/clear API)
- [ ] Add SpanProcessor + LogRecordProcessor for run_id enrichment
- [ ] Ensure run_id set at pipeline entry points
- [ ] Verify logs and spans carry run_id consistently

---

## Scope 5 — Cross-process propagation (extract parallelism)

**Intent**: Preserve trace context across process boundaries so worker spans link to the parent trace.

**Representative pattern**:

```python
# parent
carrier: dict[str, str] = {}
propagate.inject(carrier)

# worker
context = propagate.extract(carrier)
with tracer.start_as_current_span("extract.worker", context=context):
    ...
```

**Target files**:
- `src/extract/parallel.py`
- `src/extract/parallel_executor.py` (if exists)

**Deletions**:
- None.

**Checklist**:
- [ ] Inject propagation carrier before dispatch
- [ ] Extract in worker initializer
- [ ] Start worker spans with parent context or link
- [ ] Keep fallback behavior for unsupported propagators

---

## Scope 6 — Logging correlation + log instrumentation alignment

**Intent**: Enable OTel logging instrumentation so `OTEL_PYTHON_LOG_CORRELATION` is effective and aligns with existing structured logging.

**Representative pattern**:

```python
from opentelemetry.instrumentation.logging import LoggingInstrumentor

if log_correlation:
    LoggingInstrumentor().instrument(set_logging_format=False)
```

**Target files**:
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/logs.py`

**Deletions**:
- Remove manual `trace_id`/`span_id` injection in logs if log correlation is enabled (avoid duplication).

**Checklist**:
- [ ] Enable logging instrumentation when log correlation is true
- [ ] Ensure logging format does not break on missing fields
- [ ] Avoid double-injecting trace/span IDs

---

## Scope 7 — Metrics units + Views alignment

**Intent**: Standardize duration metrics to seconds and update Views/bucket boundaries to match semconv guidance.

**Representative pattern**:

```python
meter.create_histogram("codeanatomy.stage.duration", unit="s")
View(
    instrument_name="codeanatomy.stage.duration",
    aggregation=ExplicitBucketHistogramAggregation([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]),
)
```

**Target files**:
- `src/obs/otel/metrics.py`

**Deletions**:
- Remove millisecond-based metric names or convert to seconds consistently.

**Checklist**:
- [ ] Update duration histograms to seconds
- [ ] Adjust bucket boundaries accordingly
- [ ] Update Views to match new units
- [ ] Ensure emitted metrics remain low-cardinality

---

## Scope 8 — System + process metrics instrumentation

**Intent**: Provide CPU/memory/process telemetry via `opentelemetry-instrumentation-system-metrics` when enabled.

**Representative pattern**:

```python
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
SystemMetricsInstrumentor().instrument()
```

**Target files**:
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/config.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Add config toggle for system metrics
- [ ] Instrument only when MeterProvider configured
- [ ] Ensure it does not run in test mode unless explicitly enabled

---

## Scope 9 — Instrumentation scope identity (version + schema_url)

**Intent**: Ensure tracers/meters/loggers include version and optional schema URL for backend attribution.

**Representative pattern**:

```python
tracer = trace.get_tracer(
    scope_name,
    instrumenting_library_version=__version__,
    schema_url=OTEL_SCHEMA_URL,
)
```

**Target files**:
- `src/obs/otel/tracing.py`
- `src/obs/otel/metrics.py`
- `src/obs/otel/logs.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Add consistent scope metadata constants
- [ ] Inject version into tracer/meter/loggers
- [ ] Wire schema URL via env/config

---

## Scope 10 — Sampler extensibility + declarative config

**Intent**: Support custom samplers via entry points and respect `OTEL_EXPERIMENTAL_CONFIG_FILE` when set.

**Representative pattern**:

```python
if os.getenv("OTEL_EXPERIMENTAL_CONFIG_FILE"):
    return configure_via_config_file(...)

sampler = load_sampler_from_entrypoints(name, arg)
```

**Target files**:
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Add entrypoint sampler resolution fallback
- [ ] Respect config file mode if enabled
- [ ] Preserve current default sampler behavior

---

## Scope 11 — Semconv stability opt‑in for HTTP/RPC

**Intent**: Enforce consistent semantic convention mode when enabling HTTP/RPC instrumentations to avoid duplicate metrics.

**Representative pattern**:

```python
if os.getenv("OTEL_SEMCONV_STABILITY_OPT_IN") is None:
    os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http"
```

**Target files**:
- `src/obs/otel/bootstrap.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Set semconv stability opt-in when auto-instrumentation is enabled
- [ ] Document expected mode in code comments (no external docs update required)

---

## Scope 12 — Span event enrichment for stage lifecycle

**Intent**: Add explicit span events for major stage boundaries so trace timelines are self‑describing.

**Representative pattern**:

```python
span.add_event("stage.start", attributes={"stage": stage})
span.add_event("stage.end", attributes={"status": status})
```

**Target files**:
- `src/obs/otel/tracing.py`
- `src/obs/otel/hamilton.py`

**Deletions**:
- None.

**Checklist**:
- [ ] Emit stage start/end events
- [ ] Include status and duration attributes
- [ ] Ensure event attributes are normalized

---

## Deferred deletions (only after full scope completion)

**Intent**: Avoid removing legacy behavior until all scopes above are fully implemented and validated.

**Candidates for deletion**:
- Any manual trace/span injection in `src/obs/otel/logs.py` that becomes redundant once log correlation instrumentation is enabled
- Legacy metric names in milliseconds after seconds‑based migration
- Hard-coded propagator defaults once env-based propagation is verified

**Checklist**:
- [ ] Confirm all scopes 1–12 are complete
- [ ] Verify telemetry output parity in a test run
- [ ] Remove redundant legacy paths cleanly

---

## Notes

- The plan intentionally targets a **design-phase ideal**: it favors correctness, specification compliance, and uniformity, even if it introduces breaking changes to emitted telemetry names or attributes.
- Documentation updates are intentionally excluded per current direction; only code changes are required.
