# OpenTelemetry Best-in-Class Extension Plan (Design Phase, Breaking Changes OK)

Date: 2026-01-29  
Owner: Codex (design phase)  
Status: complete  

## Purpose
Elevate the existing OpenTelemetry implementation to full best‑in‑class parity with the OTel Python configuration surface, semantic conventions, and collector governance features. This plan focuses on configuration plane completeness, Collector pipeline governance, semconv alignment, and advanced signal correlation features (exemplars, span→metrics), with breaking changes allowed.

## Design Principles
- **Config-plane parity**: behave according to OTel spec rules and expose the full SDK surface (env vars, limits, config file semantics).
- **Collector as policy plane**: sampling, redaction, and derived metrics live in the Collector, not in app code.
- **Stable semantics**: prefer stable semconv keys and consistent instrumentation scope names.
- **Signal correlation**: traces, logs, and metrics should correlate via run_id and exemplars.
- **Explicit lifecycle**: deterministic startup/shutdown and predictable override precedence.

---

## Scope 1 — Configuration plane parity (spec + Python SDK)

### Objective
Bring the CodeAnatomy bootstrap/config layer into full parity with the OTel spec env var contract and Python SDK configuration surface. Ensure `OTEL_EXPERIMENTAL_CONFIG_FILE` behaves correctly and all relevant env vars are honored with spec-compliant parsing.

### Representative code pattern
```python
config = resolve_otel_config()
providers = configure_otel(
    service_name="codeanatomy",
    options=OtelBootstrapOptions(
        enable_auto_instrumentation=config.auto_instrumentation,
    ),
)
```

```python
# spec-compliant bool parsing (only "true" == True)
raw = os.environ.get("OTEL_SDK_DISABLED")
if raw is not None and raw.strip().lower() not in {"true", "false", ""}:
    _LOGGER.warning("Invalid OTEL_SDK_DISABLED=%s", raw)
```

### Target files to modify
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/attributes.py`
- `src/obs/otel/logs.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Enforce spec parsing rules for `OTEL_*` booleans (only "true" is true; warn on invalid values).
- [x] Add `OTEL_LOG_LEVEL` handling for SDK internal logging.
- [x] Implement `OTEL_METRICS_EXEMPLAR_FILTER` wiring.
- [x] Implement `OTEL_LOGRECORD_ATTRIBUTE_*` limits and apply to log attributes.
- [x] Add `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` and `..._DEFAULT_HISTOGRAM_AGGREGATION` support.
- [x] Expose `OTEL_PYTHON_ID_GENERATOR` and `OTEL_PYTHON_CONTEXT` when set.
- [x] Handle `OTEL_EXPERIMENTAL_CONFIG_FILE` deterministically:
  - If file is set and providers already configured, honor it.
  - If file is set and no providers exist, either (a) parse/instantiate from file, or (b) fail fast (design decision; choose one path and document).

---

## Scope 2 — Resource identity + semconv alignment

### Objective
Align Resource attributes with stable semconv keys and ensure detector-driven enrichment is reliable and explicit.

### Representative code pattern
```python
resource = build_resource(
    service_name="codeanatomy",
    options=ResourceOptions(
        environment=environment,
        instance_id=resolve_service_instance_id(),
        attributes={"codeanatomy.repo_hash": repo_hash},
    ),
)
```

### Target files to modify
- `src/obs/otel/resources.py`
- `src/obs/otel/resource_detectors.py`
- `src/obs/otel/bootstrap.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Switch to stable `deployment.environment.name` (not incubating `deployment.environment`).
- [x] Document and enable detector set via `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` (process/os/container/k8s/cloud as available).
- [x] Ensure resource override precedence remains deterministic (code overrides env, then detector enrichment, then explicit overrides).

---

## Scope 3 — Collector pipeline upgrades (governance + derived signals)

### Objective
Upgrade the Collector pipeline to provide best‑in‑class governance and derived metrics (RED metrics from spans), plus stronger redaction/transform policies.

### Representative code pattern
```yaml
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [attributes/sanitize, transform/normalize, tail_sampling, batch]
      exporters: [otlp]
    metrics:
      receivers: [otlp, spanmetrics]
      processors: [attributes/sanitize, batch]
      exporters: [otlp]

connectors:
  spanmetrics:
    metrics_flush_interval: 15s
```

### Target files to modify
- `tools/otel/collector.yaml`

### Modules to delete
- None.

### Implementation checklist
- [x] Add `spanmetrics` connector to derive RED metrics from traces.
- [x] Add `resource`/`transform` processor for canonical attribute mapping and redaction policy. (Implemented via `resource/normalize` + `attributes/sanitize`.)
- [x] Enable k8s resource enrichment where applicable.
- [x] Ensure tail sampling and attribute sanitization remain consistent with new processors.

---

## Scope 4 — Trace semantics: sampling visibility + DB semconv

### Objective
Make sampling decisions observable and align DataFusion span attributes with database semconv.

### Representative code pattern
```python
class CodeAnatomySampler(Sampler):
    def should_sample(...):
        result = self._delegate.should_sample(...)
        attrs = {"otel.sampling.rule": "default"}
        return SamplingResult(result.decision, attrs, result.trace_state)
```

```python
span.set_attribute("db.system.name", "datafusion")
span.set_attribute("db.operation.name", "execute")
```

### Target files to modify
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/tracing.py`
- `src/datafusion_engine/execution_facade.py`
- `src/datafusion_engine/plan_bundle.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add a custom sampler wrapper to stamp `otel.sampling.rule` or equivalent.
- [x] Ensure sampling‑relevant attributes are passed at span creation time.
- [x] Adopt DB semconv attributes for DataFusion spans (low‑cardinality names only).

---

## Scope 5 — Metrics exemplars + aggregation policy

### Objective
Enable trace exemplars for histograms and expose aggregation/temporality preferences.

### Representative code pattern
```python
meter_provider = MeterProvider(
    resource=resource,
    metric_readers=[reader],
    views=metric_views(),
    exemplar_filter=exemplar_filter,
)
```

### Target files to modify
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/config.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Wire `OTEL_METRICS_EXEMPLAR_FILTER` into the MeterProvider.
- [x] Expose `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` and histogram aggregation choice.

---

## Scope 6 — Logs pipeline hardening

### Objective
Apply log record limits and avoid double logging when auto‑instrumentation is enabled.

### Representative code pattern
```python
if os.environ.get("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED") == "true":
    # Skip attaching LoggingHandler to avoid double export
    return
```

### Target files to modify
- `src/obs/otel/logs.py`
- `src/obs/otel/bootstrap.py`
- `src/obs/otel/attributes.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Enforce log record attribute count/length limits in normalization.
- [x] Honor `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED` to prevent duplicate exporters.

---

## Scope 7 — Auto‑instrumentation boundaries + semconv stability opt‑in

### Objective
Make auto‑instrumentation configuration explicit and safe, and widen semconv stability opt‑in beyond HTTP where relevant.

### Representative code pattern
```python
if state.enable_auto:
    os.environ.setdefault("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", "urllib,urllib3")
    os.environ.setdefault("OTEL_PYTHON_EXCLUDED_URLS", ".*health.*")
    os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http,db")
```

### Target files to modify
- `src/obs/otel/bootstrap.py`
- `docs/architecture/observability.md`

### Modules to delete
- None.

### Implementation checklist
- [x] Define default auto‑instrumentation exclusions at bootstrap.
- [x] Expand `OTEL_SEMCONV_STABILITY_OPT_IN` to include DB/messaging as needed.
- [x] Document supported auto‑instrumentation knobs and boundaries.

---

## Scope 8 — Tests and contracts for new behavior

### Objective
Expand contract tests to assert new config behavior, sampler stamping, and Collector‑derived metrics assumptions.

### Representative code pattern
```python
span = spans[0]
assert span.attributes.get("otel.sampling.rule") is not None
```

### Target files to modify
- `tests/obs/test_otel_traces_contract.py`
- `tests/obs/test_otel_metrics_contract.py`
- `tests/obs/test_otel_logs_contract.py`
- `tests/obs/_support/otel_harness.py`

### Modules to delete
- None.

### Implementation checklist
- [x] Add tests for config env var precedence and parsing rules.
- [x] Add tests for log record limits and exemplar configuration wiring.
- [x] Add tests for sampler‑stamped attributes.

---

## Scope 9 — Deferred deletions and cleanup (post‑scope)

### Objective
Identify legacy paths that can be deleted only after all scopes are complete.

### Candidate deletions (defer until all scopes land)
- Any legacy log correlation utilities superseded by OTel log correlation.
- Deprecated env‑parsing helpers for OTEL variables once spec‑compliant parsing is in place.
- Redundant per‑module config fallbacks that bypass `configure_otel`.

### Implementation checklist
- [x] Confirm all entry points use `configure_otel` and no legacy config paths remain.
- [x] Remove deprecated helpers only after new config semantics are verified in tests. (No legacy helpers remained.)

---

## Acceptance Gates
- Config plane matches spec + Python SDK surface (env vars, parsing, limits, config file semantics).
- Collector pipeline includes spanmetrics and normalization processors.
- Resource identity uses stable semconv keys.
- Sampling decisions are observable and DB semconv attributes are applied.
- Exemplars and OTLP metrics preferences are wired.
- Tests cover new config, sampler, log limits, and exemplar wiring.
