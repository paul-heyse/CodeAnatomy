# A) Configuration surface area you didn’t enumerate (spec + Python-specific)

## Mental model (configuration planes + precedence edges)

OpenTelemetry “configuration” is not a single API; it’s a **multi-plane control surface**:

1. **Spec env-var plane (cross-language contract)**: `OTEL_*` names + parsing rules + default semantics; may be implemented in-SDK or by a separate “autoconfigure” component. ([OpenTelemetry][1])
2. **Language SDK plane (Python runtime objects)**: `TracerProvider`/`MeterProvider`/`LoggerProvider`, processors/readers/exporters, limits, etc. Env vars generally override equivalent code config, *when the language’s autoconfig wiring is in play*. ([OpenTelemetry][2])
3. **Python zero-code agent plane** (`opentelemetry-instrument`): CLI args + `OTEL_*` env vars + **Python-specific** `OTEL_PYTHON_*` knobs that affect monkey-patching and instrumentation behavior. ([OpenTelemetry][3])
4. **Declarative config plane** (`OTEL_EXPERIMENTAL_CONFIG_FILE`): structured config file → `Parse` → config model → `Create` SDK components; *when enabled, it suppresses the flat env-var scheme* (except env-var substitution referenced inside the file). ([OpenTelemetry][1])

Two precedence “gotchas” to internalize early:

* **Env vars > code** as a general SDK-config rule. ([OpenTelemetry][2])
* **But** in Python, `Resource.create({...})` merges `OTEL_RESOURCE_ATTRIBUTES` with *lower priority* than explicitly passed resource attrs (so code can override env for resource keys). ([OpenTelemetry Python][4])
  And `OTEL_SERVICE_NAME` is a “convenience” override for `service.name` (wins over `OTEL_RESOURCE_ATTRIBUTES=service.name=...`). ([OpenTelemetry][5])

Your attached overview only shows a *thin slice* of this plane (a minimal env-var set + a couple agent vars).

---

## A1) Spec env vars: parsing rules + “global” controls

### What you get (full surface area)

**Parsing / validity rules (these matter in production IaC):**

* Empty value == unset (no “empty string disables” semantics). ([OpenTelemetry][1])
* Boolean: only case-insensitive `"true"` is true; everything else is false (including unset/empty). ([OpenTelemetry][1])
* Enums: case-insensitive; unknown values must warn + be ignored. ([OpenTelemetry][1])
* Numerics: unparseable should warn + be ignored (treated as unset). ([OpenTelemetry][1])
* Env-based config must have a **direct code-equivalent** (don’t design “env-only” behavior). ([OpenTelemetry][1])

**Global toggles / identity:**

* `OTEL_SDK_DISABLED=true` → switch to no-op SDK for *signals*; **does not affect propagators configured via `OTEL_PROPAGATORS`**. ([OpenTelemetry][1])
* `OTEL_LOG_LEVEL` → SDK internal logger verbosity. ([OpenTelemetry][1])
* `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME` → resource identity (service/environment/etc). ([OpenTelemetry][5])
* `OTEL_PROPAGATORS` → comma-separated propagators; default `tracecontext,baggage`; known values include `b3`, `b3multi`, `jaeger`, `xray`, `none`, etc. ([OpenTelemetry][1])

**Sampling (head sampling selection contract):**

* `OTEL_TRACES_SAMPLER` accepted values (incl. `jaeger_remote`, `parentbased_jaeger_remote`, `xray`, etc.). ([OpenTelemetry][5])
* `OTEL_TRACES_SAMPLER_ARG` shape depends on sampler; for remote Jaeger it’s a comma-delimited kv list (`endpoint=...,pollingIntervalMs=...,initialSamplingRate=...`). ([OpenTelemetry][1])

**Exporter selection (signal routing):**

* `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` select exporter(s) per signal; implementations MAY accept comma-separated lists; `none` disables auto-config for that signal. ([OpenTelemetry][1])
* Spec also flags legacy `logging` as deprecated-backcompat and introduces in-development `otlp/stdout`. ([OpenTelemetry][1])

### Why it matters

* These vars define the **portable “ops ABI”** (Terraform/Helm/K8s manifests can configure many services uniformly).
* Parsing rules prevent “silent misconfig” (e.g., `OTEL_SDK_DISABLED=1` is **false**, not truthy). ([OpenTelemetry][1])
* The “code-equivalent” requirement is a design constraint: anything you can do via env vars should be reproducible by constructing the same pipeline objects. ([OpenTelemetry][1])

### Minimal pattern: “portable baseline”

Your doc already shows the minimal baseline (`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_PROPAGATORS`, `OTEL_TRACES_SAMPLER(_ARG)`, `OTEL_EXPORTER_OTLP_ENDPOINT`).
This section expands what else exists beyond that baseline.

---

## A2) Pipeline tuning env vars: batching processors + metric readers

### What you get (full surface area)

**BatchSpanProcessor (traces):**

* `OTEL_BSP_SCHEDULE_DELAY` (ms), `OTEL_BSP_EXPORT_TIMEOUT` (ms), `OTEL_BSP_MAX_QUEUE_SIZE`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` (must be ≤ queue). ([OpenTelemetry][1])

Python code-equivalent constructor surface explicitly matches these knobs:
`BatchSpanProcessor(exporter, max_queue_size=..., schedule_delay_millis=..., max_export_batch_size=..., export_timeout_millis=...)`. ([OpenTelemetry Python][6])

**BatchLogRecordProcessor (logs):**

* `OTEL_BLRP_SCHEDULE_DELAY`, `OTEL_BLRP_EXPORT_TIMEOUT`, `OTEL_BLRP_MAX_QUEUE_SIZE`, `OTEL_BLRP_MAX_EXPORT_BATCH_SIZE` with same invariants. ([OpenTelemetry][1])
  Python implements a parallel constructor surface (same parameter names) for `BatchLogRecordProcessor`. ([GitHub][7])

**Push-metrics periodic reader:**

* `OTEL_METRIC_EXPORT_INTERVAL` (ms) and `OTEL_METRIC_EXPORT_TIMEOUT` (ms) govern periodic collection/export for push exporters. ([OpenTelemetry][1])
  Python’s `PeriodicExportingMetricReader(exporter, export_interval_millis=..., export_timeout_millis=...)` is the direct code-equivalent. ([OpenTelemetry Python][8])

**Metrics exemplar filter:**

* `OTEL_METRICS_EXEMPLAR_FILTER` with values `always_on|always_off|trace_based` (default `trace_based` in the spec env-var catalog). ([OpenTelemetry][1])

### Why it matters

* These knobs are your **backpressure contract**: once you have sufficient traffic, batching defaults become correctness-affecting (drop vs block vs latency).
* For LLM agents building “production-realistic” harnesses, these env vars define the *minimal test matrix*: you can deterministically reproduce drops/timeouts by shrinking queues/timeouts.

### Minimal code-equivalent patterns

**Trace batching from env vars (mechanically faithful):**

```python
import os
from opentelemetry.sdk.trace.export import BatchSpanProcessor

bsp = BatchSpanProcessor(
    exporter,
    max_queue_size=int(os.getenv("OTEL_BSP_MAX_QUEUE_SIZE", "2048")),
    schedule_delay_millis=int(os.getenv("OTEL_BSP_SCHEDULE_DELAY", "5000")),
    max_export_batch_size=int(os.getenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "512")),
    export_timeout_millis=int(os.getenv("OTEL_BSP_EXPORT_TIMEOUT", "30000")),
)
```

([OpenTelemetry][1])

**Metrics periodic reader from env vars:**

```python
import os
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

reader = PeriodicExportingMetricReader(
    exporter,
    export_interval_millis=int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")),
    export_timeout_millis=int(os.getenv("OTEL_METRIC_EXPORT_TIMEOUT", "30000")),
)
```

([OpenTelemetry][1])

### Footguns

* Setting `*_MAX_EXPORT_BATCH_SIZE` > `*_MAX_QUEUE_SIZE` violates the spec constraint (expect rejection/fallback depending on language). ([OpenTelemetry][1])
* Empty env values are treated as unset, so `OTEL_BSP_MAX_QUEUE_SIZE=""` does **not** mean “0 / disable”. ([OpenTelemetry][1])

---

## A3) Cardinality + memory guardrails: limits env vars

### What you get (full surface area)

Spec-defined truncation/limits controls:

* **Attribute limits**: `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT`, `OTEL_ATTRIBUTE_COUNT_LIMIT`. ([OpenTelemetry][1])
* **Span limits**:

  * span attributes: `OTEL_SPAN_ATTRIBUTE_*`
  * structural: `OTEL_SPAN_EVENT_COUNT_LIMIT`, `OTEL_SPAN_LINK_COUNT_LIMIT`
  * per-event/link attribute counts: `OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT`, `OTEL_LINK_ATTRIBUTE_COUNT_LIMIT`. ([OpenTelemetry][1])
* **LogRecord limits**: `OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT`, `OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT`. ([OpenTelemetry][1])

Implementation guidance: SDKs should only expose env vars for attribute types where they actually implement truncation. ([OpenTelemetry][1])

### Why it matters

* Limits are the only **spec-standard** way to prevent “high-cardinality attribute poisoning” from turning into unbounded memory/CPU on the hot path.
* LLM agents that generate instrumentation code should treat these as **system invariants**: e.g., instrumentation that assumes it can attach 10k events per span will be silently truncated.

### Footguns

* Many “cardinality problems” are *not* solved by limits because the explosion often occurs at the **attribute-key space** level; limits cap *count per span/log record*, not “unique label cardinality across time”. Treat limits as a safety net, not a design excuse.

---

## A4) OTLP exporter configuration: endpoint/protocol/TLS/compression/retry (full matrix)

Your doc sets only `OTEL_EXPORTER_OTLP_ENDPOINT` in examples; the spec surface is substantially larger.

### A4.1 Endpoint semantics (base vs per-signal; HTTP path construction)

* Base endpoint: `OTEL_EXPORTER_OTLP_ENDPOINT` (all signals). For OTLP/HTTP, SDKs **append** `/v1/traces|metrics|logs` when using the base endpoint. ([OpenTelemetry][9])
* Signal-specific endpoints: `OTEL_EXPORTER_OTLP_{TRACES|METRICS|LOGS}_ENDPOINT` override base; when set, the URL is used “as-is” (no auto path rewriting). ([OpenTelemetry][9])
* Defaults differ by transport: OTLP/gRPC commonly uses `http://localhost:4317`; OTLP/HTTP uses `http://localhost:4318`. ([OpenTelemetry][9])

### A4.2 Protocol selection

* `OTEL_EXPORTER_OTLP_PROTOCOL` + per-signal variants select `grpc` vs `http/protobuf` vs `http/json`. ([OpenTelemetry][9])

### A4.3 Headers (auth + routing metadata)

* `OTEL_EXPORTER_OTLP_HEADERS` + per-signal variants; encoded as comma-separated `k=v` pairs; values treated as strings; similar to W3C baggage encoding (without extra metadata). ([OpenTelemetry][9])

### A4.4 Timeouts, compression, TLS, mTLS

* Timeouts: `OTEL_EXPORTER_OTLP_TIMEOUT` + per-signal. ([OpenTelemetry][9])
* Compression: `OTEL_EXPORTER_OTLP_COMPRESSION` + per-signal; spec names `gzip` and supports disabling via `none`. ([OpenTelemetry][10])
* gRPC “insecure” toggle: `OTEL_EXPORTER_OTLP_INSECURE` + per-signal; applies only when endpoint is provided without `http/https` scheme; scheme can override insecure choice. ([OpenTelemetry][10])
* TLS trust + mTLS:

  * trust store: `OTEL_EXPORTER_OTLP_CERTIFICATE` (+ per-signal)
  * client key/cert: `OTEL_EXPORTER_OTLP_CLIENT_KEY`, `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` (+ per-signal). ([OpenTelemetry][10])

### A4.5 Metrics-OTLP specific export behavior toggles

* `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`: `cumulative|delta|lowmemory` (case-insensitive). ([OpenTelemetry][11])
* `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION`: `explicit_bucket_histogram|base2_exponential_bucket_histogram`. ([OpenTelemetry][11])

### A4.6 Retry behavior (OTLP exporter contract)

* Transient errors must be retried with exponential backoff + jitter to avoid overwhelming receivers during outages. ([OpenTelemetry][10])

### A4.7 User-Agent / distro identifiers

* Exporters should emit a User-Agent identifying exporter + language + version; spec allows adding a “distribution” product identifier prefix (relevant when you ship a distro/agent). ([OpenTelemetry][10])

### Minimal patterns

**Pattern A — single endpoint, multi-signal, OTLP/HTTP:**

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318"
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer%20…"
```

(Expect `/v1/traces|metrics|logs` to be derived from the base endpoint.) ([OpenTelemetry][9])

**Pattern B — per-signal override to avoid path surprises:**

```bash
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://collector-a:4318/v1/traces"
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://collector-b:4318/v1/metrics"
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT="http://collector-a:4318/v1/logs"
```

Per-signal endpoints are used as-is. ([OpenTelemetry][9])

### Footguns

* If you set `OTEL_EXPORTER_OTLP_ENDPOINT` to a URL that already includes a path, OTLP/HTTP appends `/v1/<signal>` relative to that path; per-signal endpoint vars are the escape hatch when you need exact control. ([OpenTelemetry][9])
* `OTEL_EXPORTER_OTLP_HEADERS` is not “HTTP header syntax”; it’s a constrained `k=v,k=v` encoding. ([OpenTelemetry][10])

---

## A5) Python SDK-specific env vars (beyond the cross-language spec)

### What you get (full surface area)

**Resource merge and precedence:**

* `OTEL_RESOURCE_ATTRIBUTES` is merged into `Resource.create(...)` but has *lower priority* than explicitly supplied attributes. ([OpenTelemetry Python][4])
* `OTEL_SERVICE_NAME` is a convenience alias for `service.name` and wins when both are set. ([OpenTelemetry Python][4])

**Experimental resource detectors:**

* `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`: comma-separated detector names resolved via `opentelemetry_resource_detector` entry points; explicitly experimental (name/behavior may break). ([OpenTelemetry Python][4])

**Python’s “env var catalog” includes additional knobs** (e.g., Prometheus exporter host/port, Jaeger exporter extras, exemplar filter, etc.), so `opentelemetry.sdk.environment_variables` is effectively the authoritative enumeration for “what Python exposes today.” ([OpenTelemetry Python][4])

---

## A6) Python zero-code agent configuration (`opentelemetry-instrument`): CLI↔env + `OTEL_PYTHON_*`

### What you get (full surface area)

**CLI↔env mapping rule (mechanical):**

* Any CLI property can be expressed as env var by uppercasing + prefixing with `OTEL_` (e.g., `exporter_otlp_endpoint` → `OTEL_EXPORTER_OTLP_ENDPOINT`). ([OpenTelemetry][3])

**Python-specific `OTEL_PYTHON_*` categories:**

1. **URL exclusion (global + per-library):**

* `OTEL_PYTHON_EXCLUDED_URLS` (comma-separated regexes)
* `OTEL_PYTHON_<LIB>_EXCLUDED_URLS` for specific libs; documented libs include Django/Falcon/FastAPI/Flask/Pyramid/Requests/Starlette/Tornado/urllib/urllib3. ([OpenTelemetry][3])

2. **Request attribute extraction (framework-specific):**

* `OTEL_PYTHON_DJANGO_TRACED_REQUEST_ATTRS`
* `OTEL_PYTHON_FALCON_TRACED_REQUEST_ATTRS`
* `OTEL_PYTHON_TORNADO_TRACED_REQUEST_ATTRS` ([OpenTelemetry][3])

3. **Logging controls (agent-level):**

* `OTEL_PYTHON_LOG_CORRELATION` (inject trace context into logs)
* `OTEL_PYTHON_LOG_FORMAT`, `OTEL_PYTHON_LOG_LEVEL`
* `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED` (attach OTLP handler to root logger). ([OpenTelemetry][3])

4. **Other high-leverage toggles:**

* `OTEL_PYTHON_DJANGO_INSTRUMENT=false` (disable default-enabled Django instrumentation)
* `OTEL_PYTHON_ELASTICSEARCH_NAME_PREFIX`
* `OTEL_PYTHON_GRPC_EXCLUDED_SERVICES`
* `OTEL_PYTHON_ID_GENERATOR` (e.g., xray)
* `OTEL_PYTHON_INSTRUMENTATION_SANITIZE_REDIS`
* `OTEL_PYTHON_AUTO_INSTRUMENTATION_EXPERIMENTAL_GEVENT_PATCH=patch_all`. ([OpenTelemetry][3])

5. **Disable specific instrumentations by entry-point name:**

* `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=...` (comma-separated). ([OpenTelemetry][3])

**Programmatic auto-instrumentation (same agent behavior, different lifecycle hook):**

* `from opentelemetry.instrumentation.auto_instrumentation import initialize; initialize()` is the documented escape hatch when you need to initialize **inside worker processes after fork** (and must run before importing frameworks like FastAPI due to patch timing). ([OpenTelemetry][12])

### Minimal code pattern: “post-fork initialize”

```python
from opentelemetry.instrumentation.auto_instrumentation import initialize

initialize()  # call in worker after fork, before importing instrumented frameworks

from fastapi import FastAPI
app = FastAPI()
```

([OpenTelemetry][12])

### Footguns

* Agent patches are import-time sensitive; if an LLM agent generates “initialize after importing FastAPI”, you get partial/no instrumentation. ([OpenTelemetry][12])
* Log correlation is a **logging-instrumentation concern** (log record factory + formatting), not “OTLP exporter magic”; mismatched formatters are a common cause of “trace_id missing” confusion. ([OpenTelemetry Python Contrib][13])

---

## A7) Declarative configuration (`OTEL_EXPERIMENTAL_CONFIG_FILE`) + Configuration SDK model

### What you get (full surface area)

**Spec env var contract:**

* `OTEL_EXPERIMENTAL_CONFIG_FILE=/path/to/config.yaml` enables file-driven SDK construction. If set, *all other SDK configuration env vars are ignored* (except env-var substitution explicitly referenced by the config). ([OpenTelemetry][1])

**Config SDK mechanics (conceptual pipeline):**

* `Parse(file) → configuration model` (must perform env-var substitution; must treat “missing” vs “present-but-null” distinctly)
* `Create(model) → {TracerProvider, MeterProvider, LoggerProvider, Propagators, ConfigProvider}`
* `ComponentProvider` mechanism for custom plugin implementations (exporters/processors/samplers/etc). ([OpenTelemetry][14])

### Why it matters

* It is the **only** approach that scales past a few dozen env vars without turning deployment config into a flat-string swamp, especially when you need multi-exporter pipelines, custom processors, or org-specific redaction policy.

### Practical caveat (for Python docs you’ll write)

Support is optional by language; the OpenTelemetry docs explicitly point you at the implementation compliance matrix when env var support matters. ([OpenTelemetry][5])

---

## A8) SemConv stability opt-in (config surface, not “semconv theory”)

Even though this belongs to your later “semconv governance” chapter, it is still configuration:

* `OTEL_SEMCONV_STABILITY_OPT_IN` is designed as a comma-separated set of categories (e.g., `http`, `databases`, `messaging`) to control “emit stable vs legacy” conventions during transition windows. ([OpenTelemetry][15])
  Your overview already references `http|http/dup`; the advanced doc should treat this as an explicit config plane with versioning rules and downstream dashboard compatibility strategy.

---

**Template reference used for structure:**  
**Baseline overview being extended:** 

[1]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/ "Environment Variable Specification | OpenTelemetry"
[2]: https://opentelemetry.io/docs/languages/sdk-configuration/ "SDK Configuration | OpenTelemetry"
[3]: https://opentelemetry.io/docs/zero-code/python/configuration/ "Agent Configuration | OpenTelemetry"
[4]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/environment_variables.html "opentelemetry.sdk.environment_variables — OpenTelemetry Python  documentation"
[5]: https://opentelemetry.io/docs/languages/sdk-configuration/general/ "General SDK Configuration | OpenTelemetry"
[6]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.export.html?utm_source=chatgpt.com "opentelemetry.sdk.trace.export"
[7]: https://github.com/open-telemetry/opentelemetry-python/blob/main/opentelemetry-sdk/src/opentelemetry/sdk/_logs/_internal/export/__init__.py?utm_source=chatgpt.com "sdk - open-telemetry/opentelemetry-python"
[8]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.export.html?utm_source=chatgpt.com "opentelemetry.sdk.metrics.export"
[9]: https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/ "OTLP Exporter Configuration | OpenTelemetry"
[10]: https://opentelemetry.io/docs/specs/otel/protocol/exporter/ "OpenTelemetry Protocol Exporter | OpenTelemetry"
[11]: https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/otlp/ "Metrics Exporter - OTLP | OpenTelemetry"
[12]: https://opentelemetry.io/docs/zero-code/python/troubleshooting/ "Troubleshooting Python automatic instrumentation issues | OpenTelemetry"
[13]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html?utm_source=chatgpt.com "OpenTelemetry Logging Instrumentation"
[14]: https://opentelemetry.io/docs/specs/otel/configuration/sdk/ "Configuration SDK | OpenTelemetry"
[15]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/?utm_source=chatgpt.com "Semantic conventions for HTTP metrics"


# B) Tracing: advanced SDK mechanics & extension points (Python)

## Mental model for this whole section

**Tracing runtime = 3 interlocked pipelines**:

1. **Control plane (provider construction)**: `TracerProvider(sampler, id_generator, span_limits, active_span_processor, resource, …)` defines *policy* and *plumbing*. ([OpenTelemetry Python][1])
2. **Data plane (span lifecycle)**: `Tracer.start_span / start_as_current_span` → SDK span creation algorithm (IDs, sampling, recording vs non-recording) → events/attrs/links/status/exception recording → span end. ([OpenTelemetry Python][1])
3. **Egress plane (processing + export)**: `SpanProcessor` hooks (`on_start`, `on_end`) → `SpanExporter.export(batch)` with flush/shutdown semantics; “multi-export” is just multiple processors/exporters. ([OpenTelemetry Python][1])

Key invariant: **sampling is decided at span creation time**; later mutations can’t retroactively affect that decision. ([OpenTelemetry Python][2])

---

## B1) `TracerProvider` / `Tracer`: construction knobs + instrumentation scope identity

### What you get (full surface area)

* `TracerProvider(sampler=None, resource=None, shutdown_on_exit=True, active_span_processor=None, id_generator=None, span_limits=None)` is the **hard boundary** between “API calls” and “SDK policy”. ([OpenTelemetry Python][1])
* `get_tracer(instrumenting_module_name, instrumenting_library_version=None, schema_url=None, attributes=None)` emits **instrumentation scope identity** (name/version/schema/attrs) onto spans; return value may be noop vs real; tracer instance caching is intentionally unspecified. ([OpenTelemetry Python][1])
* Naming rule (important): `instrumenting_module_name` should be a **stable, importable constant**; *not* `__name__`; and should name the *instrumentation*, not the instrumented library (e.g., `"opentelemetry.instrumentation.requests"`, not `"requests"`). ([OpenTelemetry Python][1])

### Why it matters (systems use cases)

* **Scope identity is your provenance primitive**: backends can attribute spans to a particular instrumentation package/version; dashboards/alert regressions often reduce to “scope drift”. ([OpenTelemetry Python][1])
* “Multiple tracers” inside one process are normal; “multiple tracer providers” usually isn’t (it fragments policy + export lifecycle).

### Configuration / mechanics

* `TracerProvider` input is used to create/store an `InstrumentationScope` on created `Tracer`s (spec-level requirement). ([OpenTelemetry][3])
* `shutdown_on_exit=True` implies SDK-managed process-exit shutdown; if you own lifecycle (workers, embedded runtimes), treat shutdown explicitly (`provider.shutdown()`, `provider.force_flush()`). ([OpenTelemetry Python][1])

### Minimal patterns

```python
import importlib.metadata
from opentelemetry.sdk.trace import TracerProvider

SCOPE_NAME = "myco.observability"  # stable constant

provider = TracerProvider()
tracer = provider.get_tracer(
    instrumenting_module_name=SCOPE_NAME,
    instrumenting_library_version=importlib.metadata.version("myco-observability"),
)
```

([OpenTelemetry Python][1])

### Footguns

* `__name__` differs per file → “same library, many tracer names” → backend cardinality + confusing provenance. ([OpenTelemetry Python][1])
* Changing scope names/versions casually breaks longitudinal comparisons more reliably than changing span names.

---

## B2) SDK span creation: IDs, sampling, recording vs exporting

### What you get (full surface area)

**Spec-defined span creation algorithm (ordering is contractual):**

1. If parent trace ID valid → reuse; else generate new trace ID **before** sampling (sampler needs a trace ID).
2. Call `Sampler.should_sample(...)`.
3. Generate a new span ID **regardless** of sampling decision (so other components can rely on uniqueness even for non-recording spans).
4. Create a recording or non-recording span depending on `SamplingResult`. ([OpenTelemetry][3])

**Recording vs sampled is two-dimensional**:

* SDK distinguishes `IsRecording` (collects attrs/events/links/status) vs trace flags `Sampled` (export eligibility).
* Reaction matrix (critical):

  * `IsRecording=true, Sampled=true` → SpanProcessors **and** exporters see it
  * `IsRecording=true, Sampled=false` → SpanProcessors see it, exporters **should not**
  * `IsRecording=false` → SpanProcessors should not see it; `IsRecording=false & Sampled=true` is “not allowed” ([OpenTelemetry][3])

Python-level sampling decisions encode this as `Decision.DROP | RECORD_ONLY | RECORD_AND_SAMPLE` and `SamplingResult(decision, attributes, trace_state)`. ([OpenTelemetry Python][2])

### Why it matters (systems use cases)

* `RECORD_ONLY` is a **local policy hook**: you can run SpanProcessors (e.g., local redaction/enrichment/metrics derivation) without exporting spans. ([OpenTelemetry][3])
* The “generate span ID even for non-recording spans” invariant supports correlation primitives (e.g., logs referencing an active span) without requiring export. ([OpenTelemetry][3])

### Configuration / mechanics

* Python `Sampler.should_sample(...)` receives `attributes` and `links` (plus `trace_state`), enabling **attribute-aware** and **link-aware** head sampling. ([OpenTelemetry Python][2])
* Samplers can attach **attributes at creation time** via `SamplingResult(attributes=...)`, and can emit a `trace_state` (potentially modified) in the `SamplingResult`. ([OpenTelemetry Python][2])
* Sampler implementers are explicitly nudged to use the **parent span context’s `TraceState`** (not just the raw `trace_state` parameter) and to treat sampling as “creation-time only” (though future changes are discussed). ([OpenTelemetry Python][2])

### Minimal patterns

**Creation-time attributes matter for sampling (don’t set later if the sampler needs them):**

```python
with tracer.start_as_current_span(
    "http.server",
    attributes={"http.route": "/health", "otel.sample.hint": "low_value"},
):
    ...
```

(Set attributes at creation; samplers can only consider what’s present then.) ([OpenTelemetry Python][1])

### Footguns

* `Span.set_attribute` later can’t influence sampling; Python docs explicitly recommend setting attributes at span creation for this reason. ([OpenTelemetry Python][1])
* `Span.update_name()` post-start can destabilize name-based policies; sampling behavior after rename is implementation-dependent. ([OpenTelemetry Python][1])

---

## B3) Sampling: built-ins, env wiring, custom sampler plug-in points

### What you get (full surface area)

**Built-in sampler classes (Python SDK):**

* Static: `ALWAYS_ON`, `ALWAYS_OFF`
* Probabilistic: `TraceIdRatioBased(rate)`
* Parent-based wrappers: `ParentBased(root=…)`, plus convenience `ParentBasedTraceIdRatio` ([OpenTelemetry Python][2])

**Env-driven sampler selection (Python doc’s supported values):**

`OTEL_TRACES_SAMPLER ∈ { always_on, always_off, traceidratio, parentbased_always_on (default), parentbased_always_off, parentbased_traceidratio }` and `OTEL_TRACES_SAMPLER_ARG` supplies rate for ratio samplers (range `[0.0, 1.0]`). ([OpenTelemetry Python][2])

**Configurable custom sampler via entry points**:

* Entry point group: `opentelemetry_traces_sampler`
* Factory signature: `Callable[[str], Sampler]`
* `OTEL_TRACES_SAMPLER` selects the entry point key; `OTEL_TRACES_SAMPLER_ARG` is passed through (empty string if unset). ([OpenTelemetry Python][2])

### Why it matters

* You can implement **route-aware sampling**, “drop noisy spans”, or vendor-specific policies without forking the SDK—then control it with env vars like any built-in. ([OpenTelemetry Python][2])

### Configuration / mechanics

* `SamplingResult` can add span attributes at creation time—use this to stamp *why* something was (not) sampled (`otel.sampling.rule=...`). ([OpenTelemetry Python][2])
* `TraceIdRatioBased` assumes trace ID randomness; Python’s `IdGenerator` docs call out that samplers rely on randomness in generated IDs. ([OpenTelemetry Python][4])

### Minimal patterns

**Custom sampler (attribute-aware drop + delegate):**

```python
from opentelemetry.sdk.trace.sampling import Sampler, SamplingResult, Decision, ParentBased, TraceIdRatioBased

class DropHealthcheckSampler(Sampler):
    def __init__(self, delegate: Sampler) -> None:
        self._delegate = delegate

    def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None):
        route = (attributes or {}).get("http.route") or ""
        if route == "/health":
            return SamplingResult(Decision.DROP)
        return self._delegate.should_sample(parent_context, trace_id, name, kind, attributes, links, trace_state)

    def get_description(self) -> str:
        return "DropHealthcheckSampler"

sampler = ParentBased(root=DropHealthcheckSampler(TraceIdRatioBased(0.1)))
```

([OpenTelemetry Python][2])

**Custom sampler via env vars (packaged entry point):** follow the `opentelemetry_traces_sampler` factory pattern shown in the Python sampling docs. ([OpenTelemetry Python][2])

### Footguns

* Don’t “sample on attributes you set later”; if you need attribute-based sampling, enforce “attrs-at-start” in your instrumentation conventions. ([OpenTelemetry Python][1])
* “Randomness” matters: non-random trace IDs degrade ratio sampling correctness; the Trace SDK spec has evolving guidance around trace randomness / explicit randomness and IdGenerator signaling. ([OpenTelemetry][3])

---

## B4) SpanProcessor pipeline: ordering, parallelism, and hot-path constraints

### What you get (full surface area)

**SpanProcessor contract (Python):**

* `on_start(span, parent_context)` and `on_end(span)` are called **synchronously on the thread** that starts/ends the span → must not block and must not throw. ([OpenTelemetry Python][1])
* Registration order is significant; processors invoked in the order registered. ([OpenTelemetry Python][1])

**Built-in “multi” processors (Python SDK):**

* `SynchronousMultiSpanProcessor`: fan-out sequentially, preserves order. ([OpenTelemetry Python][1])
* `ConcurrentMultiSpanProcessor(num_threads=2)`: fan-out in parallel via thread pool; still invoked synchronously from caller thread but offloads underlying work. ([OpenTelemetry Python][1])

**Provider flush semantics are nontrivial**:

* `TracerProvider.force_flush(timeout_millis)` by default flushes processors sequentially; later processors get less time; using `ConcurrentMultiSpanProcessor` can flush in parallel at the cost of threads. ([OpenTelemetry Python][1])

### Why it matters

* SpanProcessor is the **primary extension point** for org-level policy: redaction, span name normalization, attribute curation, span-to-metrics derivation, export routing (when you want multi-export), etc. ([OpenTelemetry][3])
* If you block in `on_start/on_end`, you block the application request path.

### Configuration / mechanics

* Processors run only when spans are recording (spec). ([OpenTelemetry][3])
* Batch exporters belong behind processors (e.g., `BatchSpanProcessor(exporter)`), not inside your business logic. ([OpenTelemetry Python][5])

### Minimal patterns

**Parallel fan-out to multiple exporters (reduce flush starvation):**

```python
from opentelemetry.sdk.trace import TracerProvider, ConcurrentMultiSpanProcessor
from opentelemetry.sdk.trace.export import BatchSpanProcessor

active = ConcurrentMultiSpanProcessor(num_threads=4)
provider = TracerProvider(active_span_processor=active)

active.add_span_processor(BatchSpanProcessor(otlp_exporter))
active.add_span_processor(BatchSpanProcessor(debug_exporter))
```

([OpenTelemetry Python][1])

**Custom SpanProcessor skeleton (hot-path safe by construction):**

```python
from opentelemetry.sdk.trace import SpanProcessor

class EnrichOnStart(SpanProcessor):
    def on_start(self, span, parent_context=None):
        # must not block / throw; keep it O(1) and allocation-light
        span.set_attribute("myco.proc", "enrich_on_start")

    def on_end(self, span):
        # span is ReadableSpan at end; treat as read-only / non-mutating
        pass
```

([OpenTelemetry Python][1])

### Footguns

* `on_start/on_end` throwing exceptions violates the contract; treat processors as “never-fail policy code”. ([OpenTelemetry Python][1])
* “Sequential flush starvation”: if you register many processors and call `force_flush(timeout=…)`, the tail processors can systematically fail flush under load unless you use the concurrent multi-processor. ([OpenTelemetry Python][1])

---

## B5) Exporters: `SpanExporter` contract + batch vs simple processors

### What you get (full surface area)

* `SpanExporter.export(spans: Sequence[ReadableSpan]) -> SpanExportResult` plus `shutdown()` and `force_flush(timeout_millis)`; exporters must be wired via `SimpleSpanProcessor` or `BatchSpanProcessor`. ([OpenTelemetry Python][5])
* `SimpleSpanProcessor` exports ended spans directly; `BatchSpanProcessor` batches ended spans, configurable via constructor and env vars, and implements its own shutdown/flush. ([OpenTelemetry Python][5])

### Why it matters

* Exporter is your integration boundary to “anything”: OTLP, vendor APIs, file sinks, internal queues. Keep it isolated behind processors for lifecycle + backpressure.

### Minimal patterns

**Custom exporter stub (test sink / integration point):**

```python
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

class MyExporter(SpanExporter):
    def export(self, spans):
        # spans is Sequence[ReadableSpan]
        # return FAILURE to signal export failed
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis=30000) -> bool:
        return True
```

([OpenTelemetry Python][5])

### Footguns

* If your exporter blocks (network/disk), you must put it behind `BatchSpanProcessor` (or implement your own async queue). ([OpenTelemetry Python][5])
* `SpanExporter.force_flush()` is a “hint” contract; don’t assume hard real-time guarantees. ([OpenTelemetry Python][5])

---

## B6) SpanLimits: hard caps + truncation (Python-specific behavior)

### What you get (full surface area)

Python exposes a `SpanLimits(...)` object and threads it through `TracerProvider(span_limits=…)` → `Span(limits=…)`. ([OpenTelemetry Python][1])

Key behaviors (Python docs are explicit):

* The class **does not enforce limits itself**; it centralizes: user args → env vars → defaults. ([OpenTelemetry Python][1])
* All limit args must be non-negative int, `None`, or `SpanLimits.UNSET`; `UNSET = -1`. ([OpenTelemetry Python][1])
* **Precedence**: model-specific limit overrides corresponding global limit; else defaults apply. ([OpenTelemetry Python][1])
* Env-var mappings include:

  * `OTEL_ATTRIBUTE_COUNT_LIMIT` (max attrs across span/event/link)
  * `OTEL_SPAN_EVENT_COUNT_LIMIT`, `OTEL_SPAN_LINK_COUNT_LIMIT`
  * `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT` (span-only)
  * plus attribute length truncation knobs (max attribute length; span attribute length). ([OpenTelemetry Python][1])

### Why it matters

* This is the **SDK-level kill switch** against “instrumentation-generated memory bombs” (events/links/attrs) and runaway string payloads.

### Minimal patterns

```python
from opentelemetry.sdk.trace import TracerProvider, SpanLimits

limits = SpanLimits(
    max_span_attributes=64,
    max_events=128,
    max_links=16,
    max_attribute_length=256,  # truncation
)

provider = TracerProvider(span_limits=limits)
```

([OpenTelemetry Python][1])

### Footguns

* Limits constrain *per-span payload*, not *global label cardinality over time*; you still need semantic conventions + attribute hygiene.
* Attribute values of `None` are “undefined behavior” and strongly discouraged; avoid emitting them. ([OpenTelemetry Python][1])

---

## B7) IdGenerator: trace/span ID generation + backend compatibility (AWS X-Ray)

### What you get (full surface area)

* Python `IdGenerator` interface: `generate_span_id() -> int` (64-bit), `generate_trace_id() -> int` (128-bit). ([OpenTelemetry Python][4])
* Randomness requirement: implementations should keep at least the low 64 bits uniformly random; `TraceIdRatioBased` relies on this randomness. ([OpenTelemetry Python][4])
* Vendor compatibility example: AWS X-Ray ID generator (contrib) embeds timestamp; configure by passing `id_generator=AwsXRayIdGenerator()` to `TracerProvider`. ([OpenTelemetry Python Contrib][6])

### Why it matters

* If you need a backend that enforces an ID format, **IdGenerator is the correct hook** (not “manually set trace IDs” hacks). ([OpenTelemetry Python Contrib][6])

### Minimal pattern

```python
import opentelemetry.trace as trace
from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
from opentelemetry.sdk.trace import TracerProvider

trace.set_tracer_provider(TracerProvider(id_generator=AwsXRayIdGenerator()))
```

([OpenTelemetry Python Contrib][6])

### Footguns

* Non-random trace IDs + ratio sampling is a correctness trap; the Trace SDK spec’s “random flag / explicit randomness” guidance is actively evolving—treat custom ID generators as “sampling-sensitive components”. ([OpenTelemetry][3])

---

## B8) Context + propagation as tracing extension points (execution units + remote boundaries)

### What you get (full surface area)

**Context API (Python):**

* `attach(context) -> Token`, `detach(token)`, `get_current()`, `create_key()`, `get_value/set_value` for cross-cutting concern state. ([OpenTelemetry Python][7])
* `detach(token)` semantics are token-based to help detect wrong call order (spec). ([OpenTelemetry][8])

**Propagation API (Python):**

* Global textmap propagator: `propagate.get_global_textmap()` / `set_global_textmap()`; `extract()`/`inject()` use configured propagator. ([OpenTelemetry Python][9])
* `OTEL_PROPAGATORS` controls `CompositePropagator` composition via `opentelemetry_propagator` entry points (e.g., `tracecontext,baggage` by default). ([OpenTelemetry Python][9])
* Propagators spec: `TextMapPropagator` uses `Getter`/`Setter` helpers that should be **stateless constants** to avoid allocations; extract must not throw on parse failure and must preserve existing valid context. ([OpenTelemetry][10])

### Why it matters

* Context propagation is what makes “distributed trace” a trace; `Tracer` creation alone only gives you local trees. ([OpenTelemetry][10])

### Minimal patterns

**Manual HTTP-ish extraction → child span with explicit parent context:**

```python
from opentelemetry import propagate, trace

ctx = propagate.extract(carrier=incoming_headers)  # propagator decides how
with trace.get_tracer("myco.observability").start_as_current_span(
    "handle_request",
    context=ctx,
):
    ...
```

([OpenTelemetry Python][9])

**Span links for async fan-in (message queues / batch consumers):**

```python
from opentelemetry import trace

tracer = trace.get_tracer("myco.observability")

# capture upstream context from an earlier span
ctx = trace.get_current_span().get_span_context()
link = trace.Link(ctx)

with tracer.start_as_current_span("consumer", links=[link]):
    ...
```

([OpenTelemetry][11])

### Footguns

* Context misuse across async boundaries often shows up as “detach token mismatch” or spans not parenting correctly; treat `attach/detach` tokens as non-optional correctness checks. ([OpenTelemetry Python][7])

---

## B9) Span data-plane controls: events, links, status, exceptions, mutability

### What you get (full surface area)

* `Tracer.start_as_current_span(..., links=(), start_time=None, record_exception=True, set_status_on_exception=True, end_on_exit=True)` controls “automatic exception → event/status” behavior. ([OpenTelemetry Python][1])
* `Span.is_recording()` is the hot-path guard: if false, avoid expensive attribute/event computation. ([OpenTelemetry Python][1])
* `Span.record_exception(exc, ...)` records exception as a span event; `Span.set_status(Status(...))` sets explicit status; status codes are `{UNSET, OK, ERROR}`. ([OpenTelemetry Python][1])
* `TraceState` is immutable and W3C-governed; mutations create new copies; propagators/exporters may emit modified copies right before serialization (spec). ([OpenTelemetry][12])

### Why it matters

* You can make “error handling” deterministic: either rely on `set_status_on_exception` default behavior or disable it and enforce explicit status setting (useful in frameworks that convert exceptions → responses). ([OpenTelemetry Python][1])

### Footguns

* Assuming “ERROR status automatically creates an exception event” is not a spec guarantee; exception events are a separate API call/behavior (Python provides `record_exception` and the `record_exception=True` context-manager option). ([OpenTelemetry Python][1])

---

## B10) Instrumentation suppression (advanced noise control)

### What you get (full surface area)

* Auto-instrumentation libraries may honor a `suppress_instrumentation` context manager (from `opentelemetry.instrumentation.utils`) to skip generating spans for code paths like internal health checks. ([OpenTelemetry Python Contrib][13])

### Minimal pattern

```python
from opentelemetry.instrumentation.utils import suppress_instrumentation

with suppress_instrumentation():
    do_internal_call_that_would_otherwise_be_auto_instrumented()
```

([OpenTelemetry Python Contrib][13])

### Footguns

* Not all instrumentations consistently respect suppression (version drift / implementation gaps are real); treat as “best effort” unless you enforce it with tests against your exact instrumentation set. ([GitHub][14])

[1]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html "opentelemetry.sdk.trace package — OpenTelemetry Python  documentation"
[2]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.sampling.html "opentelemetry.sdk.trace.sampling — OpenTelemetry Python  documentation"
[3]: https://opentelemetry.io/docs/specs/otel/trace/sdk/ "Tracing SDK | OpenTelemetry"
[4]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.id_generator.html "opentelemetry.sdk.trace.id_generator — OpenTelemetry Python  documentation"
[5]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.export.html "opentelemetry.sdk.trace.export — OpenTelemetry Python  documentation"
[6]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/sdk-extension/aws/aws.html "OpenTelemetry Python - AWS SDK Extension — OpenTelemetry Python Contrib  documentation"
[7]: https://opentelemetry-python.readthedocs.io/en/stable/api/context.html "opentelemetry.context package — OpenTelemetry Python  documentation"
[8]: https://opentelemetry.io/docs/specs/otel/context/?utm_source=chatgpt.com "Context"
[9]: https://opentelemetry-python.readthedocs.io/en/stable/api/propagate.html "opentelemetry.propagate package — OpenTelemetry Python  documentation"
[10]: https://opentelemetry.io/docs/specs/otel/context/api-propagators/ "Propagators API | OpenTelemetry"
[11]: https://opentelemetry.io/docs/languages/python/instrumentation/?utm_source=chatgpt.com "Instrumentation"
[12]: https://opentelemetry.io/docs/specs/otel/trace/api/ "Tracing API | OpenTelemetry"
[13]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/redis/redis.html?utm_source=chatgpt.com "OpenTelemetry Redis Instrumentation"
[14]: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/3454?utm_source=chatgpt.com "Issue #3454 · open-telemetry/opentelemetry-python-contrib"

# C) Metrics: advanced capabilities you didn’t cover

## Mental model (why metrics config is “harder” than traces)

Metrics are **pre-aggregated timeseries streams**, not event logs. The data model is explicitly built around **semantics-preserving transformations** (temporal reaggregation, spatial reaggregation, delta↔cumulative conversion) that can happen in-SDK or in the Collector. ([OpenTelemetry][1])
A “metric stream” identity is grouped by **Resource + Instrumentation Scope + name + type + unit + description + intrinsic point properties (temporality/monotonic)**; individual timeseries are then distinguished by **Attributes**. (Histogram bucket boundaries / exponential scale are *not* identifying properties.) ([OpenTelemetry][1])

In the SDK, the **MeterProvider owns the pipeline** (Views, MetricReaders, MetricExporters) and is required to apply configuration uniformly to already-created Meters if it supports updates. ([OpenTelemetry][2])

---

## C1) `MeterProvider`: multi-reader pipelines + “disable-by-default” + scope hygiene

### What you get

* Python `MeterProvider(metric_readers=…, resource=…, exemplar_filter=…, views=…, shutdown_on_exit=True)`; each `MetricReader` is *independent* and collects *separate* streams of metrics (i.e., multiple parallel pipelines). ([OpenTelemetry Python][3])
* “Disable instruments by default” is a **View-level policy**: configure a match-all View with `DropAggregation()` then selectively re-enable instruments with additional Views. ([OpenTelemetry Python][3])
* Meter naming rules mirror tracing: do not use `__name__`; use a stable instrumentation scope name/version, because scope participates in metric identity. ([OpenTelemetry Python][3])

### Minimal pattern: multi-reader + default deny

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, InMemoryMetricReader
from opentelemetry.sdk.metrics.view import View, DropAggregation

# exporter omitted here; plug OTLP / console / vendor exporter into PeriodicExportingMetricReader(exporter)
push_reader = PeriodicExportingMetricReader(exporter)
test_reader = InMemoryMetricReader()

provider = MeterProvider(
    metric_readers=[push_reader, test_reader],
    views=[
        View(instrument_name="*", aggregation=DropAggregation()),  # default deny
        View(instrument_name="http.server.duration"),              # allow-list
    ],
)

metrics.set_meter_provider(provider)
meter = metrics.get_meter("myco.observability", version="1.2.3")
```

([OpenTelemetry Python][3])

### Footguns

* Multiple readers means **multiple callback executions** (async instruments) and potentially different temporality/aggregation per reader; if your callback is expensive, you feel it N times. ([OpenTelemetry][2])

---

## C2) `MetricReader`: temporality + aggregation preference maps (Python-native control surface)

### What you get

Python’s `MetricReader(preferred_temporality=…, preferred_aggregation=…)` accepts **maps keyed by instrument class**:

* `preferred_temporality: dict[type, AggregationTemporality]` (default: cumulative for all instrument classes). ([OpenTelemetry Python][4])
* `preferred_aggregation: dict[type, Aggregation]` (default: `DefaultAggregation` for all instrument classes); a non-default View aggregation overrides these defaults. ([OpenTelemetry Python][4])

This is the “code-first” twin of the OTLP exporter spec requirement that the exporter can influence reader temporality/aggregation by instrument kind. ([OpenTelemetry][5])

### Minimal pattern: instrument-kind temporality flip (delta where it matters)

```python
from opentelemetry.sdk.metrics import Counter, Histogram, UpDownCounter, ObservableCounter, ObservableUpDownCounter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, AggregationTemporality

preferred_temporality = {
    Counter: AggregationTemporality.DELTA,
    ObservableCounter: AggregationTemporality.DELTA,
    Histogram: AggregationTemporality.DELTA,
    UpDownCounter: AggregationTemporality.CUMULATIVE,
    ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
}

reader = PeriodicExportingMetricReader(exporter)
reader._preferred_temporality = preferred_temporality  # illustrative; wire via ctor / subclass per your style
```

(Conceptually consistent with the OTLP metrics temporality preference behavior for Delta vs Cumulative by instrument kind.) ([OpenTelemetry Python][4])

### Footguns

* Delta temporality reduces client-side state but shifts “rate math” and reset handling downstream; it is not a free win—treat it as a pipeline contract, not an optimization toggle. ([OpenTelemetry][1])

---

## C3) `PeriodicExportingMetricReader`: lifecycle and concurrency invariants (push export)

### What you get

* Spec: periodic exporting reader has `exportIntervalMillis` (default 60000ms) and `exportTimeoutMillis` (default 30000ms). ([OpenTelemetry][2])
* Python: `PeriodicExportingMetricReader(exporter, export_interval_millis=None, export_timeout_millis=None)`; if interval is `math.inf`, it won’t invoke periodic collection; exporter `export()` is not called concurrently. ([OpenTelemetry Python][4])

### Systems consequence

“Export not concurrent” means: if your exporter is slow and your interval is short, you’re effectively **serializing collection/export**, increasing collection jitter (and risking timeouts). ([OpenTelemetry Python][4])

---

## C4) Views: the primary “metrics shaping language” (selection + stream configuration + measurement processing)

### 4.1 Instrument selection (match rules)

Spec: a View selects instruments based on criteria; stream config then defines what gets produced. ([OpenTelemetry][2])
Python `View(...)` supports matching by:

* `instrument_type`
* `instrument_name` (wildcards supported; avoid wildcards if you also set `name` to prevent multi-instrument renames)
* `meter_name`, `meter_version`, `meter_schema_url`
* `instrument_unit` ([OpenTelemetry Python][6])

### 4.2 Stream configuration (rename/describe/attribute projection)

Spec requires at minimum:

* `name` (metric stream name)
* `description`
* `attribute_keys` = allow-list; keys not included must be ignored; if not specified, SDK should fall back to the instrument’s advisory `Attributes` list; else keep all. ([OpenTelemetry][2])
  Python implements:
* `name`, `description`
* `attribute_keys` (if set, only these keys identify the stream)
* `aggregation` (override)
* `exemplar_reservoir_factory` (override exemplar reservoir per stream) ([OpenTelemetry Python][6])

### 4.3 Precedence rules (View vs advisory)

If both View and instrument advisory parameters specify the same aspect (e.g., boundaries or attribute keys), **View wins**. ([OpenTelemetry][2])

### 4.4 Multi-view conflicts are not “illegal” (but must be understood)

If multiple Views match one instrument, you can intentionally produce multiple streams; however, conflicting identities can trigger warnings and can surface as “semantic errors” downstream (duplicate metric identity). The SDK should still export and warn. ([OpenTelemetry][2])

### Minimal pattern: drop noise, project attrs, rename stream, swap aggregation

```python
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.view import (
    View, DropAggregation, SumAggregation,
    ExplicitBucketHistogramAggregation, ExponentialBucketHistogramAggregation,
)

views = [
    View(instrument_name="*", aggregation=DropAggregation()),

    # rename + attribute projection for request count
    View(
        instrument_name="http.server.request_count",
        name="http.server.requests",
        attribute_keys={"http.status_code"},
        aggregation=SumAggregation(),
    ),

    # choose exponential hist for latency (or explicit with custom boundaries)
    View(
        instrument_name="http.server.duration",
        aggregation=ExponentialBucketHistogramAggregation(max_size=160, max_scale=20),
    ),
]

provider = MeterProvider(metric_readers=[reader], views=views)
```

([OpenTelemetry Python][3])

---

## C5) Aggregations beyond “default”: explicit bucket vs exponential histogram (and why you care)

### What you get

Spec requires SDKs to provide: Drop, Default, Sum, LastValue, ExplicitBucketHistogram; and *should* provide Base2 Exponential Bucket Histogram. ([OpenTelemetry][2])
Python `DefaultAggregation` maps instrument kinds to aggregations (Histogram → ExplicitBucketHistogram; ObservableGauge → LastValue; Counters → Sum). ([OpenTelemetry Python][6])

### Exponential (base2) histogram: configuration and behavior

Spec Base2 Exponential Bucket Histogram aggregation:

* `MaxSize` default 160 buckets per positive/negative range
* `MaxScale` default 20
* `RecordMinMax` default true
* requires handling full IEEE normal float range; should avoid non-normal values in sum/min/max because they don’t map to valid buckets. ([OpenTelemetry][2])
  Python exposes `ExponentialBucketHistogramAggregation(max_size=160, max_scale=20)` and also export-side `ExponentialHistogramDataPoint` structures. ([OpenTelemetry Python][6])

### Why it matters

* Exponential hist is the cost/accuracy sweet spot for long-tail latencies (dynamic scale + bounded bucket count), especially when you can’t pre-pick boundaries that fit both “1ms p50” and “60s p99.9”. ([OpenTelemetry][2])

---

## C6) Temporality: per-instrument-kind contracts + OTLP exporter knobs

### What you get

* Python `AggregationTemporality ∈ {DELTA, CUMULATIVE, UNSPECIFIED}`. ([OpenTelemetry Python][4])
* OTLP Metrics Exporter spec requires configuration to influence MetricReader output temporality and default histogram aggregation by instrument kind; it also defines env vars:

  * `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` (default cumulative)
  * `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION` (default explicit bucket histogram) ([OpenTelemetry][5])

Delta preference semantics (spec-defined): Delta for Counter/Async Counter/Histogram, Cumulative for UpDownCounter/Async UpDownCounter. ([OpenTelemetry][5])

### Why it matters

Temporality is the “state placement decision”:

* **Cumulative**: SDK holds state; downstream can be stateless for rate math.
* **Delta**: SDK can be more stateless; downstream must handle resets/gaps and possibly convert delta→cumulative (explicitly supported as a defined transformation). ([OpenTelemetry][1])

---

## C7) Cardinality limits + overflow semantics (the real cost-control primitive)

### What you get (spec)

* Cardinality limit = hard cap on distinct attribute-set combinations (“metric points”) per metric stream per collection cycle; enforcement should happen **after** View attribute filtering to allow users to avoid limit via projection. ([OpenTelemetry][2])
* Configuration precedence:

  1. per-stream view value (`aggregation_cardinality_limit`)
  2. reader default
  3. default 2000 ([OpenTelemetry][2])
* Overflow behavior:

  * SDK must aggregate “excess” measurements into a synthetic point carrying `otel.metric.overflow=true`
  * measurements must not be double-counted or dropped under overflow
  * synchronous + cumulative: must continue exporting attribute sets observed before overflow; new sets go to overflow
  * asynchronous: should prefer first-observed attribute sets in callback when limiting ([OpenTelemetry][2])

### Why it matters

Metrics are currently **exempt from common attribute limits** because truncation/deletion affects time series identity; therefore, cardinality limiting + attribute projection are the standard guardrails. ([OpenTelemetry][2])

---

## C8) Asynchronous instruments & callback semantics: per-reader isolation + persistence rules

### What you get (spec)

* Async callbacks must be invoked **for the specific MetricReader performing collection**; observations only apply to that reader. ([OpenTelemetry][2])
* SDK should disregard using async instrument APIs outside registered callbacks; should use a timeout to prevent indefinite callback execution; must complete all callbacks for an instrument before starting the next collection round. ([OpenTelemetry][2])
* SDK should not produce aggregated metric data for previously-observed attribute sets not observed during a successful callback (persistence rules). ([OpenTelemetry][2])

### Python callback payload model

Python observable instruments take callbacks that accept `CallbackOptions` and return an iterable of `Observation` objects (including attributes), enabling “fan-out over labels” from a single callback. ([OpenTelemetry Python][3])

### Footguns

* If you attach two readers (e.g., OTLP push + Prometheus pull), your callback executes in two different scheduling regimes; write callbacks to be **idempotent, bounded, and fast**, or isolate expensive sampling behind memoization keyed by `(reader, collection_time_window)`.

---

## C9) Exemplars: trace↔metrics correlation hooks (filter + reservoir + view override)

### What you get (spec)

Exemplars are sampled measurements that carry:

* measurement value + time
* attributes **not preserved** in the metric point (i.e., filtered-out attrs)
* active span’s trace_id/span_id for correlation ([OpenTelemetry][2])

SDK must provide:

* `ExemplarFilter` (eligibility)
* `ExemplarReservoir` (storage/sampling), instantiated **per timeseries** (per stream + attribute set) ([OpenTelemetry][2])

Built-in filters: AlwaysOn, AlwaysOff, TraceBased; default should be TraceBased; filter config should follow env-var spec. ([OpenTelemetry][2])
Default reservoirs: `SimpleFixedSizeExemplarReservoir` and `AlignedHistogramBucketExemplarReservoir`, with specified default selection rules by aggregation kind; defaults may change in minor versions. ([OpenTelemetry][2])

### What Python exposes

* Filters: `AlwaysOnExemplarFilter`, `AlwaysOffExemplarFilter`, `TraceBasedExemplarFilter`. ([OpenTelemetry Python][3])
* Reservoirs: `SimpleFixedSizeExemplarReservoir`, `AlignedHistogramBucketExemplarReservoir`, plus `View(exemplar_reservoir_factory=...)` to override per stream. ([OpenTelemetry Python][3])
* Measurement APIs accept `context=` (e.g., `Counter.add(..., context=...)`, `Histogram.record(..., context=...)`), which is the mechanism exemplar sampling can use to bind trace/span identifiers to measurements. ([OpenTelemetry Python][3])

### Minimal pattern: “trace-based exemplars + attribute projection”

If a View keeps only `{http.status_code}` as stream attributes, exemplar will retain “discarded” attrs and the trace/span id so joining exemplar attrs + point attrs reconstructs the original attribute set. ([OpenTelemetry][2])

---

## C10) Instrument advisory parameters: “hints” that Views can override

### What you get (spec + Python surface)

* Advisory `ExplicitBucketBoundaries`: applies when explicit bucket histogram aggregation is used; ignored if View specifies explicit buckets; used if no View matches or View selects default aggregation. ([OpenTelemetry][2])
* Advisory `Attributes` list: recommended attribute keys; View attribute_keys takes precedence; if no View specifies keys, advisory keys should be used; else retain all. ([OpenTelemetry][2])
* Python Histogram supports `explicit_bucket_boundaries_advisory=...` at instrument creation time. ([OpenTelemetry Python][3])

---

## C11) Duplicate instrument registration: why “same name” can still break you

### What you get (spec)

Duplicate instrument registration occurs when >1 instrument of the same name is created for identical Meters but with different identifying fields (unit, description, etc). SDK must still return functional instruments and should warn; users can often resolve by renaming/setting description via Views. ([OpenTelemetry][2])
Instrument names are case-insensitive; differing casing can trigger duplication/warnings if the SDK encodes names case-sensitively. ([OpenTelemetry][2])

### Why it matters

This is the fastest path to “backend rejects semantic errors” or “mysterious duplicate streams” in dashboards—especially in large polyglot repos where multiple teams define similar instruments.

---

If you want next, the natural continuation is **D) Logs** (processor tuning + correlation + OTLP log export matrix) or **E) Auto-instrumentation** (metrics-specific knobs like HTTP duration boundaries, client/server metrics shaping, etc.).

[1]: https://opentelemetry.io/docs/specs/otel/metrics/data-model/ "Metrics Data Model | OpenTelemetry"
[2]: https://opentelemetry.io/docs/specs/otel/metrics/sdk/ "Metrics SDK | OpenTelemetry"
[3]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.html "opentelemetry.sdk.metrics package — OpenTelemetry Python  documentation"
[4]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.export.html "opentelemetry.sdk.metrics.export — OpenTelemetry Python  documentation"
[5]: https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/otlp/ "Metrics Exporter - OTLP | OpenTelemetry"
[6]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html "opentelemetry.sdk.metrics.view — OpenTelemetry Python  documentation"

# D) Logs: processor tuning + correlation + OTLP log export matrix

## Mental model (3 planes + 2 timestamps)

**Plane 0 — source logging runtime:** Python `logging` emits `LogRecord`s (level, message, exception info, timestamp, logger name, etc).

**Plane 1 — OTel “log data model”:** each emitted record becomes an OTel LogRecord with **top-level typed fields** + a free-form `Attributes` map; critically it carries **both** `Timestamp` (event time) and `ObservedTimestamp` (collector/SDK observation time).

**Plane 2 — OTel Logs SDK pipeline:** `LoggerProvider` owns config + processors; `Logger.emit()` populates trace context fields from the resolved `Context`; `LogRecordProcessor`(s) mutate/filter/batch; `LogRecordExporter` serializes/transmits (e.g., OTLP).

Python reality check: the OTel logs spec is **Stable**, but Python’s logs support is still described as **experimental / under development** in Python-facing docs; treat APIs as drift-prone and pin versions aggressively.

---

## D1) Log data model: what “advanced correctness” means

### D1.1 Field inventory (top-level) + correlation primitives

A log record’s top-level fields include: `Timestamp`, `ObservedTimestamp`, `TraceId`, `SpanId`, `TraceFlags`, `SeverityText`, `SeverityNumber`, `Body`, `Resource`, `InstrumentationScope`, `Attributes`, `EventName`.

Trace correlation contract:

* `TraceId` is optional; set when logs are part of request processing and have an assigned trace id.
* `SpanId` is optional; if present, `TraceId` **should** also be present.
* `TraceFlags` contains W3C flags (notably SAMPLED).

Time semantics:

* `ObservedTimestamp` is “time observed by collection system”; for SDK-generated logs it’s typically set at generation time and equals `Timestamp`; mapping guidance: use `Timestamp` if present else `ObservedTimestamp`.

Severity semantics:

* `SeverityNumber` normalized ranges: TRACE(1–4) … DEBUG(5–8) … INFO(9–12) … WARN(13–16) … ERROR(17–20) … FATAL(21–24); `0` = unspecified.
* ERROR semantics: ERROR(≥17) indicates erroneous situation; recommended mappings for sources without severity concepts are provided.

Body semantics (structured logging):

* `Body` supports `AnyValue`: string **or** structured maps/arrays to preserve structured logs; it’s optional and may vary per event instance.
* `Attributes`/maps must have unique keys by default; receivers may behave unpredictably with duplicates (optionally allowed for perf, but user-owned risk).

**Implication for LLM agents:** treat `Body` as the canonical “message payload” (potentially structured) and `Attributes` as the canonical enrichment space; do not “stringify structured payloads” unless downstream requires it.

---

## D2) Logs SDK: provider-owned configuration + logger-level filtering (spec)

### D2.1 `LoggerProvider` ownership + scope identity

* Provider associates a `Resource` with all LogRecords produced by any `Logger` from it.
* Provider owns configuration (processors +, in-development, configurator) and updates must apply to *already returned* `Logger`s (provider is the config indirection point).
* `GetLogger` stores an `InstrumentationScope` on the created `Logger`; invalid logger names must still return a working logger (warn; don’t throw).

### D2.2 `LoggerConfig` (Development status): minimum severity + trace-based gating

`LoggerConfig` defines (at least) three policy levers:

* `disabled` → behave as no-op.
* `minimum_severity` → drop records with `SeverityNumber != 0` and `< minimum_severity`.
* `trace_based` → if true, drop records associated with **unsampled** traces (valid `SpanId` + TraceFlags SAMPLED unset).

Emit-time filtering is mandatory even if callers don’t check `Enabled()`:

* Minimum severity + trace-based rules must be applied before processing a record.

**LLM-agent framing:** treat `LoggerConfig.trace_based` as the log analogue of “only export sampled spans”, but note its precise definition is *TraceFlags + SpanId* (not “presence of TraceId”).

---

## D3) Processor pipeline: synchronous hot path + mutation visibility + concurrency hazards

### D3.1 Pipeline topology: fan-out is first-class

* Processors register on the provider and run in **registration order**; each processor may terminate in its own exporter pipeline; SDK must allow custom processors and decorating built-ins (enrich/filter/fan-out).

### D3.2 `OnEmit` is synchronous and mutation-visible

* `OnEmit(logRecord: ReadWriteLogRecord, context)` is called synchronously on the emitting thread; must **not block** or throw.
* Mutations to `logRecord` must be visible to subsequent processors.
* `ReadWriteLogRecord` is not required to be concurrent-safe; concurrent reads/writes can race; recommended pattern: clone before concurrent processing (e.g., inside batching processor).

### D3.3 `Enabled()` as a fast path (optional) + contract nuances

* `Logger.Enabled` returns false if no processors exist; may consult processor-level `Enabled` to cheaply determine if a record would be dropped.
* Processors implementing `Enabled` must **also** handle filtering in `OnEmit` (callers can’t be expected to call `Enabled` first).

---

## D4) Built-in processors: tuning knobs + backpressure contracts (and Python drift)

### D4.1 Simple vs batch processors (spec)

* Simple processor: exports “as soon as finished”; must synchronize exporter `Export` calls (no concurrent export).
* Batching processor:

  * queues + batches and exports; must synchronize exporter `Export` calls (no concurrent export).
  * `maxQueueSize`: queue cap; once full, **logs are dropped** (hard backpressure behavior).
  * `scheduledDelayMillis`: export interval; default 1000ms (spec).
  * `exportTimeoutMillis`: cancel export after timeout; default 30000ms (spec).
  * `maxExportBatchSize`: ≤ `maxQueueSize`; default 512.

### D4.2 Env var tuning surface (spec vs Python)

Spec-defined env vars for Batch LogRecord Processor:

* `OTEL_BLRP_SCHEDULE_DELAY` default **1000ms**
* `OTEL_BLRP_EXPORT_TIMEOUT` default **30000ms**
* `OTEL_BLRP_MAX_QUEUE_SIZE` default 2048
* `OTEL_BLRP_MAX_EXPORT_BATCH_SIZE` default 512

Python-specific docs indicate **divergence**:

* `OTEL_BLRP_SCHEDULE_DELAY` default **5000ms** (Python docs, not spec default).
* `OTEL_BLRP_EXPORT_TIMEOUT` is documented as “currently does nothing” in Python (points to an issue); treat as non-functional until verified in your pinned version.

**Operational implication:** tuning guidance must be “spec-first”, then “Python-implementation verified” (your advanced doc should explicitly mark these as drift points).

### D4.3 Practical tuning heuristics (queue/drop/latency triangle)

* Lower `SCHEDULE_DELAY` → lower end-to-end log latency but higher exporter overhead (more frequent flush).
* Higher `MAX_QUEUE_SIZE` → fewer drops during exporter stalls but more memory + longer “burst catch-up” latency.
* Smaller `MAX_EXPORT_BATCH_SIZE` → reduces single-request payload size; may increase request rate.
* Always model the explicit drop point: queue overflow is **lossy** by design in batching processor.

### D4.4 `ForceFlush` and `Shutdown` (when it’s actually justified)

* Provider `Shutdown` must invoke `Shutdown` on all processors; `ForceFlush` must invoke `ForceFlush` on all processors.
* Processor `ForceFlush` is a hint to complete outstanding tasks/export ASAP, honoring timeouts; spec says only use when “absolutely necessary” (e.g., FaaS suspension).

---

## D5) Correlation: two different mechanisms that people conflate

### D5.1 Correlation in exported OTel logs (data-model native)

Logs SDK requirement: trace context fields (`TraceId`, `SpanId`, `TraceFlags`) must be populated from the resolved `Context` when the record is emitted (explicit context or current context).

This is **backend correlation**: your exported log record contains trace identifiers, independent of local text formatting.

### D5.2 Correlation in *text logs* (stdlib logging record factory injection)

The Python contrib logging instrumentation:

* registers a custom `logging` LogRecord factory that injects tracing context fields into the `logging.LogRecord` object; injected keys include `otelSpanID`, `otelTraceID`, `otelServiceName`, `otelTraceSampled`.
* is opt-in: `OTEL_PYTHON_LOG_CORRELATION=true` calls `logging.basicConfig()` with a format that prints those injected variables; the factory is still registered regardless, but without a formatter that references the keys you won’t *see* them.

This is **local formatting correlation**: it affects your rendered log lines (and any non-OTel sinks consuming those lines).

### D5.3 Trace-based log filtering (spec-level logger config)

If `LoggerConfig.trace_based=true`, logs associated with unsampled traces must be dropped (SpanId present + TraceFlags unsampled).

**Advanced pattern:** use trace-based log filtering when you treat logs as “span events but richer”, and you want log volume to track trace sampling decisions. (But note: logs without trace context bypass this filter by definition.)

---

## D6) OTLP log export matrix (env vars, protocol, TLS/mTLS, retry)

### D6.1 Exporter selection (SDK autoconfig surface)

`OTEL_LOGS_EXPORTER` selects logs exporter; default “otlp”; known values include `otlp`, `console`, deprecated `logging`, and `none` to disable logs auto-export.

### D6.2 OTLP exporter config: base vs per-signal overrides

OTLP exporter config options are required and each must be overridable by signal-specific variants (including logs).

Core knobs (all-signal + per-signal `*_LOGS_*`):

* Endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`) with explicit scheme/host/port/path handling; per-signal endpoint is used “as-is” and overrides base. For OTLP/HTTP, base endpoint must construct per-signal URLs; per-signal endpoint avoids path rewriting.
* Insecure toggle for OTLP/gRPC only when endpoint has no scheme (`OTEL_EXPORTER_OTLP_LOGS_INSECURE`).
* TLS trust: `OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE`.
* mTLS: `OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY` + `OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE`.
* Headers: `OTEL_EXPORTER_OTLP_LOGS_HEADERS` (see encoding below).
* Compression: `OTEL_EXPORTER_OTLP_LOGS_COMPRESSION` (`gzip` specified; `none` disables).
* Timeout: `OTEL_EXPORTER_OTLP_LOGS_TIMEOUT` (per-batch).
* Protocol: `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` ∈ `{grpc, http/protobuf, http/json}`; default should be `http/protobuf` unless strong reasons (back-compat) pick `grpc`.

Header encoding: env var headers must be `key1=value1,key2=value2` (W3C baggage-like, **no** semicolon metadata); all values are strings.

### D6.3 Endpoint path semantics (the most common “why are my logs not arriving?” bug class)

* With base endpoint: logs go to `.../v1/logs` (auto-appended).
* With per-signal endpoint: **no auto suffix**; you must provide the exact path you want (including `/v1/logs` for OTLP/HTTP, or root for OTLP/gRPC depending on your receiver).

### D6.4 Retry & backoff (must-have behavior)

Transient errors must be retried with exponential backoff + jitter to avoid overwhelming the destination; transient error definitions differ by OTLP/HTTP vs OTLP/gRPC. (Critical for “logs during outage” semantics.)

### D6.5 User-Agent / distro fingerprinting

OTLP exporters should emit a User-Agent identifying exporter + language + version; may allow a distribution identifier prefix (useful for fleet-level debugging of agent variants).

---

## D7) Minimal end-to-end patterns (manual SDK, then zero-code)

### D7.1 Manual: stdlib logging → OTel LoggingHandler → BatchLogRecordProcessor → OTLPLogExporter

Core wiring (OTLP exporter import path shown in a widely used reference example):

```python
import logging

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

logger_provider = LoggerProvider()
set_logger_provider(logger_provider)

exporter = OTLPLogExporter()  # configured via OTEL_EXPORTER_OTLP_* env vars
logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

handler = LoggingHandler(logger_provider=logger_provider)
logging.getLogger().addHandler(handler)

logging.warning("Something interesting happened")

logger_provider.shutdown()  # flush/export pending logs
```

(OTel docs show the same shape, substituting `ConsoleLogRecordExporter` for testing.)

### D7.2 Zero-code logs: “attach OTLP handler to root logger”

Python zero-code logs auto-instrumentation explicitly frames logs as “no Logs API; only SDK”: OTel attaches an OTLP handler to the root logger, effectively turning Python logging into an OTLP log source.
Relevant agent config knobs include: `OTEL_PYTHON_LOG_CORRELATION`, `OTEL_PYTHON_LOG_FORMAT`, `OTEL_PYTHON_LOG_LEVEL`, and `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED` (attaches OTLP handler).

---

If you want next, the natural follow-on is **E) Auto-instrumentation & instrumentation libraries (logs-specific knobs + hooks + suppression + per-lib config)**, because that’s where “logs correlation” and “what gets emitted at all” usually becomes operationally tricky.

# E) Auto-instrumentation & instrumentation libraries (logs-specific knobs + hooks + suppression + per-lib config)

## Mental model (agent ≈ “SDK autoconfig + monkey patches + per-lib shims”)

Python zero-code instrumentation is a **Python agent** that primarily uses **monkey patching** to modify library functions at runtime. ([OpenTelemetry][1])
The agent’s control surface is split:

1. **SDK autoconfig**: exporters/sampling/resources/etc (your Section A).
2. **Instrumentation selection**: which instrumentors are activated, and with what *default* kwargs (often none).
3. **Instrumentation behavior shaping**: per-lib env vars and/or explicit `instrument(..., hooks=..., excluded_urls=..., …)` calls.

Critical BaseInstrumentor invariant: `opentelemetry-instrument` calls `instrument()` **without optional args**, so “agent-friendly” customization must be expressible via **env vars** or defaults inside the instrumentor. ([OpenTelemetry Python Contrib][2])

---

## E1) Toolchain topology: `opentelemetry-distro` + `bootstrap` + `instrument` (+ programmatic init)

### E1.1 Packages & their roles

* `opentelemetry-distro` installs API+SDK + the `opentelemetry-bootstrap` and `opentelemetry-instrument` tools. ([OpenTelemetry][1])
* `opentelemetry-bootstrap -a install` scans the active `site-packages` and installs matching `opentelemetry-instrumentation-*` packages (e.g., finds `flask` ⇒ installs `opentelemetry-instrumentation-flask`). ([OpenTelemetry][1])
* `opentelemetry-instrument … python your_app.py` configures global providers/exporters and activates installed instrumentors; supports configuring exporters via CLI/env. ([PyPI][3])

### E1.2 CLI↔env mapping rule (mechanical)

Any CLI property is mappable to an env var by uppercasing and prefixing `OTEL_` (e.g., `exporter_otlp_endpoint` → `OTEL_EXPORTER_OTLP_ENDPOINT`). ([OpenTelemetry][4])

### E1.3 Advanced agent knobs (distro/configurator/id-gen)

`opentelemetry-instrumentation` exposes:

* `--distro` / `OTEL_PYTHON_DISTRO` (select distro)
* `--configurator` / `OTEL_PYTHON_CONFIGURATOR` (select configurator)
* `--id-generator` / `OTEL_PYTHON_ID_GENERATOR` (trace/span ID generator choice) ([PyPI][3])

### E1.4 Programmatic auto-instrumentation (when CLI injection isn’t possible)

```python
from opentelemetry.instrumentation import auto_instrumentation

auto_instrumentation.initialize()  # must run before importing some instrumented libs
```

Some instrumentations require `initialize()` to be called **before** importing the library they patch. ([PyPI][3])

### E1.5 Build-system edge case: `uv`

With `uv`, recommended pattern is generating requirements via `opentelemetry-bootstrap -a requirements` and reinstalling instrumentation after `uv sync` / dependency updates. ([OpenTelemetry][5])

---

## E2) Instrumentor architecture: what you can assume about every `opentelemetry-instrumentation-*`

### E2.1 `BaseInstrumentor` contract

* Instrumentations are implemented as `BaseInstrumentor` subclasses. ([OpenTelemetry Python Contrib][2])
* `instrumentation_dependencies()` declares supported library version ranges (requirements-style), gating activation to compatible environments. ([OpenTelemetry Python Contrib][2])
* `instrument(**kwargs)` and `uninstrument(**kwargs)` are the primary lifecycle methods; `opentelemetry-instrument` calls `instrument()` **without kwargs**, so direct calls with kwargs are your “manual override” escape hatch. ([OpenTelemetry Python Contrib][2])

### E2.2 “Two-tier configuration” pattern (agent vs library call)

* **Tier 1 (agent)**: env vars only; must be sufficient for safe defaults. ([OpenTelemetry Python Contrib][2])
* **Tier 2 (manual)**: pass hooks/filters/providers/flags directly into `.instrument(...)` or framework-specific helpers like `FastAPIInstrumentor.instrument_app(...)`. ([OpenTelemetry Python Contrib][6])

---

## E3) Global selection controls: enable/disable and scope-wide exclusions

### E3.1 Disable specific instrumentations (agent-level)

* `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS` = comma-separated list of **instrumentation entry point names** to exclude. Agent docs note these are typically the “entry point name” in the package’s `project.entry-points.opentelemetry_instrumentor` table. ([OpenTelemetry][4])
* PyPI also documents a special case: if the value contains `*`, no instrumentation is enabled. ([PyPI][3])

### E3.2 URL exclusions (global + per-lib)

Agent configuration supports:

* `OTEL_PYTHON_EXCLUDED_URLS` = regex list applied across instrumentations. ([OpenTelemetry][4])
* `OTEL_PYTHON_<LIB>_EXCLUDED_URLS` for specific libraries (Django/Falcon/FastAPI/Flask/Pyramid/Requests/Starlette/Tornado/urllib/urllib3). ([OpenTelemetry][4])

Requests instrumentation additionally documents `OTEL_PYTHON_REQUESTS_EXCLUDED_URLS` (with fallback to `OTEL_PYTHON_EXCLUDED_URLS`). ([OpenTelemetry Python Contrib][6])

### E3.3 Request attribute extraction (framework-level)

* `OTEL_PYTHON_{DJANGO|FALCON|TORNADO}_TRACED_REQUEST_ATTRS` extracts named fields from framework request objects and sets them as span attributes. ([OpenTelemetry][4])

---

## E4) Hook surfaces: the “extension points” you should standardize on

Most high-leverage instrumentations expose **hooks** invoked at deterministic points in the span lifecycle. Treat them as your “policy injection API” (enrichment, redaction, naming, correlation IDs).

### E4.1 Client HTTP hooks (Requests)

Requests instrumentation supports:

* `request_hook(span, request_obj)` where `request_obj` is `requests.PreparedRequest`
* `response_hook(span, request_obj, response)` where `response` is `requests.Response` ([OpenTelemetry Python Contrib][6])

Usage:

```python
from opentelemetry.instrumentation.requests import RequestsInstrumentor

def request_hook(span, req):
    if span and span.is_recording():
        span.set_attribute("myco.http.client.correlation_id", req.headers.get("x-correlation-id"))

def response_hook(span, req, resp):
    if span and span.is_recording():
        span.set_attribute("http.response.size", resp.headers.get("content-length"))

RequestsInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)
```

([OpenTelemetry Python Contrib][6])

Also supports metrics shaping via `duration_histogram_boundaries=[...]` (client duration metrics bucket boundaries). ([OpenTelemetry Python Contrib][6])

### E4.2 Server HTTP hooks (Django / Flask)

* Django: `request_hook(span, request)`, `response_hook(span, request, response)`. ([OpenTelemetry Python Contrib][7])
* Flask: `request_hook(span, environ)` and `response_hook(span, status, response_headers)` (WSGI-level). ([OpenTelemetry Python Contrib][8])

### E4.3 ASGI framework hooks (FastAPI)

FastAPI instrumentation includes both server and client message-level hooks:

* `server_request_hook(span, scope)`
* `client_request_hook(span, scope, message)`
* `client_response_hook(span, scope, message)` ([OpenTelemetry Python Contrib][9])

FastAPI also exposes `instrument_app(... excluded_urls=..., exclude_spans=..., http_capture_headers_*=...)` as a high-level “single-call wiring” function. ([OpenTelemetry Python Contrib][9])

### E4.4 urllib3 “shape the attributes” hook

urllib3 instrumentation supports `url_filter(url: str) -> str` to rewrite/normalize URL attributes (e.g., strip query params), plus request/response hooks. ([OpenTelemetry Python Contrib][10])

---

## E5) Cross-cutting HTTP header capture + sanitization (agent-friendly, but **experimental names**)

Multiple server framework instrumentations support env-var-based capture of arbitrary request/response headers as span attributes:

* `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`
* `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE` ([OpenTelemetry Python Contrib][8])

Capabilities (as documented in Flask/FastAPI):

* Header name matching supports **regex** (e.g., `Accept.*,X-.*`), and `.*` captures all. ([OpenTelemetry Python Contrib][8])
* Normalization: attribute keys follow `http.request.header.<header_name>` / `http.response.header.<header_name>` (lowercase; `-` → `_`), and values are recorded as lists. ([OpenTelemetry Python Contrib][8])
* Sanitization: `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS` redacts matched headers (`[REDACTED]`), supports regex. ([OpenTelemetry Python Contrib][8])
* The env var names are explicitly noted as **experimental and subject to change**. ([OpenTelemetry Python Contrib][8])

This is often the most efficient way to capture correlation IDs (e.g., `X-Request-Id`) without writing per-framework hooks.

---

## E6) Logs-specific instrumentation: correlation vs OTLP export (don’t conflate)

### E6.1 Trace-context injection into stdlib `logging` records (correlation)

The `opentelemetry-instrumentation-logging` integration:

* Registers a custom log record factory that injects `otelSpanID`, `otelTraceID`, `otelServiceName`, `otelTraceSampled` into `logging.LogRecord`. ([OpenTelemetry Python Contrib][11])
* Is opt-in via `OTEL_PYTHON_LOG_CORRELATION=true` (which also calls `logging.basicConfig()` to apply a formatter using the injected keys). ([OpenTelemetry Python Contrib][11])
* Supports `OTEL_PYTHON_LOG_FORMAT` and `OTEL_PYTHON_LOG_LEVEL`. ([OpenTelemetry Python Contrib][11])

**Ordering footgun:** if you set a logging format that references injected variables *before* enabling the integration, log statements can raise `KeyError` because the keys don’t exist yet. ([OpenTelemetry Python Contrib][11])

### E6.2 Logs auto-instrumentation (export)

Agent config supports `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true`, which attaches an OTLP handler to the root logger. ([OpenTelemetry][4])
OTel’s logs auto-instrumentation docs stress there is no “Logs API” in the same sense as traces/metrics; Python uses stdlib logging and OTel attaches an OTLP handler. ([OpenTelemetry][12])

**Practical separation:**

* `LOG_CORRELATION` = enrich log *records* with trace/span IDs (useful even if logs go to non-OTLP sinks). ([OpenTelemetry Python Contrib][11])
* `LOGGING_AUTO_INSTRUMENTATION_ENABLED` = export logs via OTLP (requires exporter config). ([OpenTelemetry][4])

---

## E7) Suppression: “skip spans/metrics for this code path”

### E7.1 `suppress_instrumentation()` context manager

Some instrumentations explicitly support suppressing instrumentation for specific operations. Example: Redis instrumentation shows:

```python
from opentelemetry.instrumentation.utils import suppress_instrumentation

with suppress_instrumentation():
    client.get("internal-key")
```

and documents that spans are not reported inside the suppression block. ([OpenTelemetry Python Contrib][13])

Use cases: internal health checks, cache warming, “self-calls” you don’t want in traces, background maintenance loops.

### E7.2 Reality: suppression support is instrumentation-specific

Even if `suppress_instrumentation()` exists, whether it’s honored depends on each instrumentation’s implementation (you should treat “suppression honored” as a property to test per library/version). A public issue exists complaining that SQLAlchemy instrumentation did not respect suppression in certain versions. ([GitHub][14])

---

## E8) Per-library configuration “cards” (what to document, by category)

Below is the **catalog** of the advanced config/hook surfaces you should cover for each instrumentation family, with Python examples anchored in published docs.

### E8.1 Web frameworks (server spans)

Common advanced knobs:

* `excluded_urls` / `OTEL_PYTHON_*_EXCLUDED_URLS` (regex). ([OpenTelemetry][4])
* Request/response hooks (enrichment) (Django/Flask). ([OpenTelemetry Python Contrib][7])
* HTTP header capture + sanitization env vars (Flask/FastAPI). ([OpenTelemetry Python Contrib][8])
* “Traced request attrs” extraction env vars (Django/Falcon/Tornado). ([OpenTelemetry][4])
* FastAPI: `exclude_spans` + message-level hooks + explicit `instrument_app(...)` config parameters. ([OpenTelemetry Python Contrib][9])

### E8.2 HTTP clients (client spans + client metrics)

Advanced knobs:

* request/response hooks (Requests, urllib3). ([OpenTelemetry Python Contrib][6])
* URL filtering/normalization (urllib3 `url_filter`). ([OpenTelemetry Python Contrib][10])
* Excluded URL regex env vars (Requests). ([OpenTelemetry Python Contrib][6])
* Duration histogram bucket boundaries (Requests `duration_histogram_boundaries`). ([OpenTelemetry Python Contrib][6])

### E8.3 Redis / data clients (sanitization + suppression)

* `OTEL_PYTHON_INSTRUMENTATION_SANITIZE_REDIS=true` (agent config) for query sanitization. ([OpenTelemetry][4])
* Suppression block for internal ops (Redis example). ([OpenTelemetry Python Contrib][13])

### E8.4 gRPC

* `OTEL_PYTHON_GRPC_EXCLUDED_SERVICES` = comma-separated list of services to exclude. ([OpenTelemetry][4])

### E8.5 Elasticsearch naming

* `OTEL_PYTHON_ELASTICSEARCH_NAME_PREFIX` alters operation name prefixing. ([OpenTelemetry][4])

### E8.6 Concurrency runtimes

* gevent patching: `OTEL_PYTHON_AUTO_INSTRUMENTATION_EXPERIMENTAL_GEVENT_PATCH=patch_all` triggers gevent monkeypatch before SDK init (explicitly “experimental”). ([PyPI][3])

### E8.7 SQLCommenter (context propagation via SQL text)

Flask instrumentation documents an optional **sqlcommenter** capability that appends contextual key-value pairs to queries, aiding correlation with DB logs. ([OpenTelemetry Python Contrib][8])

---

## E9) “Agent + hooks” best-practice patterns for LLM coding agents (high leverage, minimal tokens)

### Pattern 1: Keep agent env vars for fleet-wide policy; use hooks for app-local policy

* Env vars: exporter/protocol, excluded URLs, header capture/sanitize, disable instrumentations. ([OpenTelemetry][4])
* Hooks: attach correlation IDs, user/session hashing, route templating fixes, redaction of app-specific headers/payloads. ([OpenTelemetry Python Contrib][6])

### Pattern 2: Prefer env-var header capture for correlation IDs; reserve hooks for computed attributes

Header capture gives you standardized attribute keys + regex selection + sanitization. ([OpenTelemetry Python Contrib][8])
Hooks are best when the attribute is not a raw header (e.g., derived tenant ID, normalized route group, custom sampling hints).

### Pattern 3: Validate suppression behavior per instrumentation set

Treat suppression as “capability under test”; Redis docs show it working; other instrumentations may lag. ([OpenTelemetry Python Contrib][13])

[1]: https://opentelemetry.io/docs/zero-code/python/ "Python zero-code instrumentation | OpenTelemetry"
[2]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/base/instrumentor.html "opentelemetry.instrumentation.instrumentor package — OpenTelemetry Python Contrib  documentation"
[3]: https://pypi.org/project/opentelemetry-instrumentation/ "opentelemetry-instrumentation · PyPI"
[4]: https://opentelemetry.io/docs/zero-code/python/configuration/ "Agent Configuration | OpenTelemetry"
[5]: https://opentelemetry.io/docs/zero-code/python/troubleshooting/ "Troubleshooting Python automatic instrumentation issues | OpenTelemetry"
[6]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/requests/requests.html "OpenTelemetry requests Instrumentation — OpenTelemetry Python Contrib  documentation"
[7]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/django/django.html "OpenTelemetry Django Instrumentation — OpenTelemetry Python Contrib  documentation"
[8]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/flask/flask.html "OpenTelemetry Flask Instrumentation — OpenTelemetry Python Contrib  documentation"
[9]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html "OpenTelemetry FastAPI Instrumentation — OpenTelemetry Python Contrib  documentation"
[10]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/urllib3/urllib3.html "OpenTelemetry urllib3 Instrumentation — OpenTelemetry Python Contrib  documentation"
[11]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html "OpenTelemetry Logging Instrumentation — OpenTelemetry Python Contrib  documentation"
[12]: https://opentelemetry.io/docs/zero-code/python/logs-example/ "Logs Auto-Instrumentation Example | OpenTelemetry"
[13]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/redis/redis.html "OpenTelemetry Redis Instrumentation — OpenTelemetry Python Contrib  documentation"
[14]: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/3454?utm_source=chatgpt.com "Issue #3454 · open-telemetry/opentelemetry-python-contrib"

# F) Collector: advanced “policy plane” topics not covered

## Mental model (Collector = policy/router/compiler for telemetry)

The Collector is a **programmable policy plane** that compiles a declarative config into **pipelines** of components: **receivers → processors → exporters**, with **connectors** bridging pipelines and **extensions** providing out-of-band capabilities (health, auth, diagnostics, management). ([OpenTelemetry][1])
Two corollaries drive most “advanced” topics:

* **Topology is explicit**: configuring a component does nothing unless it’s referenced in `service.pipelines` (and `service.extensions` for extensions). ([OpenTelemetry][1])
* **Stateful policies impose routing constraints**: anything that needs “whole trace” or “whole service stream” semantics (tail sampling, span→metrics aggregations) forces **trace/service affinity** and therefore changes your scaling design. ([OpenTelemetry][2])

---

## F1) Configuration as a compiled artifact: sources, merges, overrides, and introspection

### F1.1 Multi-source config providers (`--config` URIs) + merge

`otelcol --config` accepts **file paths** and **config URIs**: `file:…`, `env:ENVVAR_WITH_YAML`, `yaml:…` (YAML-path bytes), `http(s)://…`. Multiple `--config` flags are merged into a final in-memory config. ([Go Packages][3])

**Embedding providers inside YAML** is first-class (e.g., `exporters: ${file:otlp-exporter.yaml}`) and participates in the same resolution/merge pipeline. ([OpenTelemetry][1])

### F1.2 Targeted overrides (`--set`) and why it matters

`--set key=value` applies *after* all config sources are resolved/merged, enabling “late binding” for environment-specific overrides without templating the base YAML. It supports nested keys (dot separators) and a distinct `::` separator inside values; it also has known limitations (e.g., keys containing literal `.` or `=`). ([Go Packages][3])

### F1.3 Environment-variable expansion inside YAML (with defaults + escaping)

Collector YAML supports `${env:VAR}` substitution, bash-style defaults `${env:VAR:-default}`, and `$$` to escape a literal `$`. ([OpenTelemetry][1])

### F1.4 “Trust but verify”: validate + print the effective config

* `otelcol validate --config=…` validates without running the service. ([OpenTelemetry][1])
* `otelcol print-config` can emit the *final resolved config* (redacted or unredacted; JSON is explicitly unstable) behind a feature gate (`otelcol.printInitialConfig`). ([Go Packages][3])
* `otelcol components` (a/k/a build-info/components) lists which components exist in the binary you’re actually running (critical when dealing with different distros). ([Go Packages][3])

**Implication for LLM agents:** treat Collector config as a **compiled artifact**: *inputs* = {N config sources + env vars + `--set` + feature gates + binary component set}. Always reason about the compiled result, not the authoring YAML.

---

## F2) Component identity, instancing, and cross-pipeline wiring semantics

### F2.1 `type[/name]` identifiers everywhere

Receivers/processors/exporters/pipelines use `type[/name]` identifiers (e.g., `otlp`, `otlp/2`, `traces`, `traces/2`) so you can instantiate multiple independent instances of a component type. ([OpenTelemetry][1])

### F2.2 Sharing vs cloning

* You *can* reference the same receiver/exporter in multiple pipelines.
* If a **processor** is referenced in multiple pipelines, **each pipeline gets a separate processor instance** (i.e., state is not shared). ([OpenTelemetry][1])

### F2.3 Connectors are first-class “edges” between pipelines

A connector acts as **exporter at the end of one pipeline** and **receiver at the start of another**; it may preserve signal type or change it (e.g., traces→metrics). ([OpenTelemetry][1])

Minimal “connector edge” shape:

```yaml
connectors:
  count: ...

service:
  pipelines:
    traces:
      receivers: [foo]
      exporters: [count]     # connector as exporter
    metrics:
      receivers: [count]     # connector as receiver
      exporters: [bar]
```

([OpenTelemetry][1])

---

## F3) Security boundary: TLS, auth, and proxy semantics (advanced wiring)

### F3.1 Auth is an **extension-based** mechanism (client + server modes)

Collector authentication is implemented via **extensions** that can be used:

* as **server authenticators** for receivers (auth incoming connections), or
* as **client authenticators** for exporters (add auth to outgoing requests). ([OpenTelemetry][1])

The docs provide a full two-sided example: receiver-side OIDC auth and agent-side `oauth2client` for the OTLP exporter. ([OpenTelemetry][1])

### F3.2 Proxy environment variables apply to net/http exporters

Exporters using Go `net/http` respect `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` set at Collector start time. ([OpenTelemetry][1])

### F3.3 Endpoint binding defaults are changing (DoS posture)

OTel docs note current defaults bind endpoints to `0.0.0.0` but intend to shift defaults to `localhost` (security hardening); config examples may not reflect future defaults. ([OpenTelemetry][1])

---

## F4) Resiliency + backpressure: exporterhelper knobs, WAL, and “where data drops”

### F4.1 Exporter helper = standardized reliability surface

Most exporters reuse a shared “exporter helper” layer that implements **queuing, batching, timeouts, and retries** with a consistent config surface. ([Go Packages][4])

Key knobs (and defaults) include:

* `retry_on_failure.{enabled, initial_interval, max_interval, max_elapsed_time, multiplier}`
* `sending_queue.{enabled, num_consumers, wait_for_result, block_on_overflow, sizer, queue_size}`
* optional `sending_queue.batch` with `{flush_timeout, min_size, max_size, sizer}` to batch *within* the queue. ([Go Packages][4])

### F4.2 When does the Collector drop data?

The official resiliency guide makes the drop mechanics explicit:

* exporter buffers to an in-memory sending queue; retries use exponential backoff + jitter (default retry window ~5 minutes);
* **drops occur** when the queue fills or retries exceed `max_elapsed_time`. ([OpenTelemetry][5])

### F4.3 Persistent queue (WAL) via `file_storage` extension

To survive Collector crashes/restarts, you can back the sending queue with disk-based WAL using `file_storage` and referencing it in `sending_queue.storage`. ([OpenTelemetry][5])

Important nuance: request context (incl. client/span context) is preserved through persistent queues, but **auth-extension context is not propagated** through persistence. ([Go Packages][4])

### F4.4 Message queues for “hard durability” hops

For maximum durability across network segments or tiers, the resiliency guide describes using Kafka (exporter on one Collector, receiver on another). ([OpenTelemetry][5])

Minimal “exporter resiliency” shape:

```yaml
extensions:
  file_storage:
    directory: /var/lib/otelcol/storage

exporters:
  otlp:
    endpoint: backend:4317
    sending_queue:
      storage: file_storage
      queue_size: 5000
    retry_on_failure:
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 10m

service:
  extensions: [file_storage]
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp]
```

([OpenTelemetry][5])

---

## F5) Scaling patterns that are *forced* by policy (gateway, two-tier, trace/service affinity)

### F5.1 Gateway pattern + why “two-tier” exists

The gateway deployment pattern centralizes OTLP ingestion; for “stateless” pipelines you can use an external L4/L7 load balancer. When telemetry must be processed in a specific Collector instance, use a **two-tier** setup with a **traceID/service-name aware load-balancing exporter** in tier 1. ([OpenTelemetry][6])

### F5.2 Load-balancing exporter: resolver + routing key

The gateway doc calls out the two core fields:

* `resolver` determines downstream endpoints (static list vs DNS-based resolution),
* `routing_key` decides the affinity key (commonly `traceID` or `service`). ([OpenTelemetry][6])

### F5.3 DNS-based scaling has eventual-consistency windows

The scaling guide notes:

* load-balancing exporter can use a DNS A record (e.g., k8s headless service) as the backend set;
* updates are eventually observed; each Collector instance may resolve at different times → transient cluster-view differences; lowering DNS resolution interval reduces the window. ([OpenTelemetry][7])

---

## F6) Stateful trace policies: tail sampling as a Collector-level decision engine

### F6.1 Tail sampling is Collector-managed and memory-stateful

Tail sampling makes decisions **after** spans are received; the tail-sampling processor buffers traces and applies policies (e.g., error-only, latency). The blog enumerates core parameters:

* `decision_wait` (default 30s),
* `num_traces` (default 50,000),
* `expected_new_traces_per_sec`. ([OpenTelemetry][2])

### F6.2 The hard constraint: all spans of a trace must hit the same instance

Tail sampling requires a “full view” of each trace; if spans for a trace are distributed across Collectors, traces fragment and policy becomes invalid. The blog explicitly recommends a two-layer deployment and points to the load-balancing exporter to ensure same-trace affinity. ([OpenTelemetry][2])

Minimal tier-2 tail sampling processor placement (conceptual):

```yaml
processors:
  tail_sampling:
    decision_wait: 30s
    num_traces: 50000
    policies:
      - name: errors
        type: status_code
        status_code:
          status_codes: [ERROR]
```

([OpenTelemetry][2])

---

## F7) Cross-signal derivations: “span-to-metrics” and service graphs via connectors (not processors)

### F7.1 Why connectors exist (and what they replaced)

OTel docs explicitly state that prior “traces→metrics processors” used a bad-practice workaround (processors exporting data directly). Connectors solve this; the legacy spanmetrics/servicegraph processors have been deprecated in favor of connectors. ([OpenTelemetry][8])

### F7.2 Policy consequence: service affinity often matters

Gateway docs call out that routing by `service` is useful for connectors like span metrics because it sends all spans of a service to the same downstream Collector, ensuring correct aggregation. ([OpenTelemetry][6])

### F7.3 Canonical connector wiring pattern

```yaml
connectors:
  spanmetrics: {}

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [spanmetrics, otlp]     # export traces, and feed connector
    metrics:
      receivers: [spanmetrics]           # connector emits metrics
      exporters: [otlp]
```

(Topology shape is guaranteed by connector semantics.) ([OpenTelemetry][1])

---

## F8) Transformation & governance plane: OTTL + high-leverage processors (and perf caveats)

The “Transforming telemetry” doc positions the Collector as the canonical place to transform data for **quality, governance, cost, security**, and warns that advanced transformations can significantly affect performance. ([OpenTelemetry][9])

High-leverage primitives to explicitly document (beyond “attributes processor exists”):

* **OTTL filtering** via `filter` processor (drop spans/logs/metrics matching conditions). ([OpenTelemetry][9])
* **Attribute mutation**: `attributes` processor (on span/metric attrs) and `resource` processor (resource attrs). ([OpenTelemetry][9])
* **Metric name/label rewrites**: `metricstransform` processor (regex-based multi-metric transforms). ([OpenTelemetry][9])
* **Infrastructure enrichment**: `resourcedetection` + `k8sattributes` (RBAC constraints called out). ([OpenTelemetry][9])
* **Structural trace mutations**: `transform` processor for span status/name rewrites and broader OTTL-driven transforms. ([OpenTelemetry][9])

---

## F9) Collector self-observability: internal telemetry as a *separate* config model

### F9.1 `service.telemetry` is its own mini-OTel SDK config

Collector config includes `service.telemetry` for the Collector’s own observability. ([OpenTelemetry][1])
The internal telemetry page makes two critical advanced points:

* Defaults: internal **metrics** exposed via Prometheus interface (default port 8888) and internal **logs** to stderr. ([OpenTelemetry][10])
* The internal telemetry config uses the OTel **declarative configuration schema**, explicitly noted as “under development” and may have breaking changes prior to a 1.0 schema. ([OpenTelemetry][10])

### F9.2 Push internal metrics/logs/traces to OTLP

The page provides explicit config patterns for:

* exporting internal metrics via OTLP reader,
* exposing Prometheus metrics on `0.0.0.0`,
* configuring metric verbosity levels (`none|basic|normal|detailed`) and **views**,
* exporting internal logs and (experimental) internal traces via OTLP. ([OpenTelemetry][10])

### F9.3 Internal metrics enumerate the “health contract” you actually monitor

Collector internal metrics cover per-component accepted/refused counts, processor in/out, exporter send/enqueue failures, and queue size/capacity—i.e., the canonical signals for diagnosing drops/backpressure. ([OpenTelemetry][10])

---

## F10) Diagnostics extensions + troubleshooting workflow (zPages/pprof + “hop-by-hop” reasoning)

### F10.1 Debug extensions

* `pprof` (port 1777) for profiling; explicitly “advanced use-case”.
* `zpages` (port 55679) for live inspection; TraceZ at `/debug/tracez`; endpoint configurable (important in containers). ([OpenTelemetry][11])

### F10.2 Pipeline-hop debugging checklist

The troubleshooting guide recommends verifying, for each hop: receiver ingestion, modification (sampling/redaction), export format, next-hop config, network policies, and checking logs and zPages. ([OpenTelemetry][11])

### F10.3 Drops: sizing vs exporter unavailability

Troubleshooting explicitly attributes common drops to undersizing and/or slow/unavailable exporter destinations, and points to configuring “queued retry options” / sending queue batch settings. ([OpenTelemetry][11])

---

## F11) Distribution engineering: component sets, custom builds, and attack-surface control

### F11.1 Custom distributions via `ocb`

OpenTelemetry documents building a custom Collector via `ocb` with a manifest YAML specifying the distribution metadata and included receivers/processors/exporters. ([OpenTelemetry][12])
The older standalone `opentelemetry-collector-builder` repository is explicitly marked deprecated (builder moved into core/release tooling). ([GitHub][13])

### F11.2 Security best practice: minimize components

Collector security guidance recommends limiting components (reduce attack surface) and explicitly calls out using `ocb` to build a minimal distribution. ([OpenTelemetry][14])

---

## F12) Fleet management plane: OpAMP + feature gates (deployment-time toggles)

### F12.1 OpAMP as the open management protocol

Collector management docs frame OpAMP as the emerging open standard for agent fleet management tasks (querying config/capabilities, upgrades, config rollout, health monitoring, TLS credential management). ([OpenTelemetry][15])
OpAMP spec explicitly includes remote config, status reporting, agent telemetry reporting, package management, and secure auto-updating + credential rotation. ([OpenTelemetry][16])
The OpAMP “state of the nation” describes both in-collector extension mode and an external supervisor model that merges remote+local config and can handle bad configs robustly. ([OpenTelemetry][17])

### F12.2 Feature gates as “early boot” behavior switches

Collector has a first-class feature gate mechanism enabling/disabling experimental/transitional behavior at deployment time, intended to govern behavior as early as possible and be queryable by components. ([Go Packages][18])

---

If you want the next section after this, the natural continuation is **G) Semantic conventions & schema governance** *as enforced/rewritten in the Collector* (metric renames, semconv stability opt-ins, “attribute hygiene” policies, and downstream dashboard compatibility contracts).

[1]: https://opentelemetry.io/docs/collector/configuration/ "Configuration | OpenTelemetry"
[2]: https://opentelemetry.io/blog/2022/tail-sampling/ "Tail Sampling with OpenTelemetry: Why it’s useful, how to do it, and what to consider | OpenTelemetry"
[3]: https://pkg.go.dev/go.opentelemetry.io/collector/service "service package - go.opentelemetry.io/collector/service - Go Packages"
[4]: https://pkg.go.dev/go.opentelemetry.io/collector/exporter/exporterhelper "exporterhelper package - go.opentelemetry.io/collector/exporter/exporterhelper - Go Packages"
[5]: https://opentelemetry.io/docs/collector/resiliency/ "Resiliency | OpenTelemetry"
[6]: https://opentelemetry.io/docs/collector/deployment/gateway/ "Gateway | OpenTelemetry"
[7]: https://opentelemetry.io/docs/collector/scaling/ "Scaling the Collector | OpenTelemetry"
[8]: https://opentelemetry.io/docs/collector/extend/custom-component/connector/ "Build a connector | OpenTelemetry"
[9]: https://opentelemetry.io/docs/collector/transforming-telemetry/ "Transforming telemetry | OpenTelemetry"
[10]: https://opentelemetry.io/docs/collector/internal-telemetry "Internal telemetry | OpenTelemetry"
[11]: https://opentelemetry.io/docs/collector/troubleshooting/ "Troubleshooting | OpenTelemetry"
[12]: https://opentelemetry.io/docs/collector/extend/ocb/ "Build a custom collector | OpenTelemetry"
[13]: https://github.com/open-telemetry/opentelemetry-collector-builder?utm_source=chatgpt.com "open-telemetry/opentelemetry-collector-builder"
[14]: https://opentelemetry.io/docs/security/config-best-practices/ "Collector configuration best practices | OpenTelemetry"
[15]: https://opentelemetry.io/docs/collector/management/ "Management | OpenTelemetry"
[16]: https://opentelemetry.io/docs/specs/opamp/ "Open Agent Management Protocol | OpenTelemetry"
[17]: https://opentelemetry.io/blog/2023/opamp-status/ "Open Agent Management Protocol (OpAMP) State of the Nation 2023 | OpenTelemetry"
[18]: https://pkg.go.dev/go.opentelemetry.io/collector/featuregate "featuregate package - go.opentelemetry.io/collector/featuregate - Go Packages"

# G) Semantic conventions & schema governance (beyond HTTP/DB)

## Mental model (SemConv = semantic ABI; SchemaURL = version tag; governance = drift control)

* **Semantic Conventions (SemConv)** define *meaningful* names/structures for spans, metrics, logs/events, and resource/entity attributes; they’re a cross-service **semantic ABI** (your backends/dashboards/alerts assume these keys/units/names). ([OpenTelemetry][1])
* SemConv is versioned + mixed-stability; schema evolution is handled via **Telemetry Schemas**: emitted data can include a **Schema URL** so consumers (Collector/backends) can transform to the version they expect. ([OpenTelemetry][2])
* Governance problem: *instrumentation upgrades* and *Collector/backend configs* are tightly coupled by attribute names. The “right” fix is **schema-aware translation**, not ad-hoc renames sprinkled everywhere. ([Go Packages][3])

---

## G1) SemConv surface map (domains you need beyond HTTP/DB)

SemConv exists **per signal** (traces/metrics/logs/events/profiles/resources) ([OpenTelemetry][1]) and **per domain**. A non-exhaustive-but-broad domain index (use as your doc’s TOC spine):

* **General**: naming, attribute/metric requirement levels, logs/events guidance, recording errors. ([OpenTelemetry][4])
* **CICD**, **CLI programs**, **CloudEvents**, **Feature Flags** (events), **GraphQL**. ([OpenTelemetry][4])
* **Messaging** (Kafka/RabbitMQ/SQS/SNS/PubSub/etc), **RPC** (gRPC/JSON-RPC/Connect), **DNS**. ([OpenTelemetry][4])
* **Exceptions** (spans + logs), **URL**, **User agent**. ([OpenTelemetry][4])
* **FaaS** (Lambda etc), **Runtime environment** (CPython/JVM/Node/.NET), **System/Container/Kubernetes**, **Hardware** metrics. ([OpenTelemetry][4])
* **Generative AI** (LLM spans/events/metrics; vendor subpages incl. OpenAI). ([OpenTelemetry][4])
* **Resource & Entities** (service/cloud/container/deployment/device/host/k8s/process/etc) for “what produced this telemetry?” identity enrichment. ([OpenTelemetry][4])

**Doc segmentation pattern (high signal):** *Domain* × *Signal*:

* “Messaging spans” vs “Messaging metrics” aren’t the same artifact set; SemConv explicitly splits by signals. ([OpenTelemetry][4])

---

## G2) Reserved attributes/events: hard interoperability anchors

The SemConv spec requires certain “reserved” attributes (e.g., `service.name`, `telemetry.sdk.*`, `exception.*`, `server.address`, `url.scheme`, etc.) and requires the `exception` event. These are ecosystem glue; treat them as **non-negotiable invariants** across migrations. ([OpenTelemetry][5])

---

## G3) SemConv “group” model + stability rules (governance primitives)

SemConv is organized into **groups** (not just flat attribute lists). Group types include `span`, `metric`, `event`, `entity`, plus `attribute_group` for auxiliary grouping. ([OpenTelemetry][6])

Each group carries:

* `id`, `brief/note`, `stability`, optional `deprecated`, and references to registry attributes. ([OpenTelemetry][6])

Stability semantics (this is what you enforce in codegen + migrations):

* Stability levels: `development`, `alpha`, `beta`, `release_candidate`, `stable` (default = `development`). ([OpenTelemetry][6])
* **Stable must not regress** (cannot move from `stable` to any other), and groups must not be removed (preserves codegen/docs for legacy instrumentations); rename/withdraw via **deprecation**. ([OpenTelemetry][6])

**Practical governance consequence:** “schema governance” is mostly “group governance”: keep group IDs stable, use deprecations, and formalize translations rather than “silent key swaps”.

---

## G4) SemConv migration controls: `OTEL_SEMCONV_STABILITY_OPT_IN`

SemConv stabilization is rolled out via opt-in categories. For HTTP (and explicitly framed as a pattern for categories like `databases`, `messaging`), instrumentations should use `OTEL_SEMCONV_STABILITY_OPT_IN` as a comma-separated list: ([OpenTelemetry][7])

* `http`: emit new stable HTTP/networking conventions, stop old experimental ones
* `http/dup`: emit **both** old and new (phased rollout); `http/dup` has precedence if both are present ([OpenTelemetry][7])

**Fleet rollout idiom (env-only):**

```bash
# phase 1: dual emit (lets dashboards/alerts migrate safely)
export OTEL_SEMCONV_STABILITY_OPT_IN="http/dup"

# phase 2: stable-only
export OTEL_SEMCONV_STABILITY_OPT_IN="http"
```

(Exact semantics + precedence are defined in the SemConv HTTP guidance.) ([OpenTelemetry][7])

---

## G5) Telemetry Schemas: version tagging + transformation contract

Telemetry schemas exist specifically so SemConv can evolve without brittle pipelines: ([OpenTelemetry][2])

* Schemas are **versioned**; each version has a unique **Schema URL**.
* Telemetry sources (instrumentations/apps) include Schema URL in emitted telemetry (work-in-progress across ecosystems).
* Consumers (Collector/backends/dashboards) may transform received telemetry from its schema version to a **target schema version** expected at point of use.
* OpenTelemetry publishes the schema transformations at a well-known URL path `/schemas/<version>`. ([OpenTelemetry][2])

Schema URL mechanics:

* It is an HTTP(S) URL for a retrievable schema file; redirects must be followed; the URL encodes **Schema Family** (prefix) + **Schema Version** (final path segment). ([OpenTelemetry][2])

---

## G6) Schema File Format (v1.0.0): what transformations exist (and therefore what “governance” can automate)

A schema file is YAML describing a schema version and the transformations required to convert from older compatible versions in the same family. ([OpenTelemetry][8])

Core structure:

* `file_format: 1.0.0`, `schema_url: /schemas/<ver>`, and `versions:` map. ([OpenTelemetry][8])
* Per version, transformations are split by target: `all`, `resources`, `spans`, `span_events`, `metrics`, `logs`. ([OpenTelemetry][8])

Supported transformation types (this defines the “governable delta”):

* `rename_attributes` (global + per-signal scoped) ([OpenTelemetry][8])
* `rename_events` (span event name changes) ([OpenTelemetry][8])
* `rename_metrics` (metric name changes) ([OpenTelemetry][8])

Ordering rules matter (don’t hand-wave):

* `all` runs before type-specific sections; `spans` transforms before `span_events`; reverse conversion applies reverse order; non-reversible renames (many→one) are breaking and should bump schema major. ([OpenTelemetry][8])

---

## G7) Collector schema translation: `schemaprocessor` (schema-aware drift firewall)

Collector-contrib provides a **Schema Processor** that translates incoming telemetry to configured target schema URLs: ([Go Packages][3])

* Matches signals by incoming **Schema URL**; on match, fetches the schema translation file (with optional `prefetch` caching) and applies transformations to emit data as the **target semantic convention version**. ([Go Packages][3])
* Targets cannot include duplicate schema families (error). ([Go Packages][3])
* Guidance: use it **at the very start** of pipelines; avoid “downgrade more than last 3 versions” (keep configs/backends updated). ([Go Packages][3])

**Minimal “schema firewall first” pipeline:**

```yaml
processors:
  schema:
    prefetch:
      - https://opentelemetry.io/schemas/1.38.0
    targets:
      - https://opentelemetry.io/schemas/1.38.0

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [schema, batch]
      exporters: [otlp]
    metrics:
      receivers: [otlp]
      processors: [schema, batch]
      exporters: [otlp]
    logs:
      receivers: [otlp]
      processors: [schema, batch]
      exporters: [otlp]
```

(Use schema targets to define the canonical SemConv version your Collector config assumes.) ([Go Packages][3])

---

## G8) SchemaURL hygiene: “if you mutate telemetry, you may invalidate its SchemaURL”

If you use processors to rename/alter telemetry that carries a valid Schema URL, you can create data that **no longer conforms** to that schema (explicitly called out by collector-contrib maintainers as a real hazard). ([GitHub][9])

Governance rule of thumb:

* If you run **schema-aware translation** (schemaprocessor) and then keep semconv-consistent transforms, SchemaURL remains meaningful.
* If you run ad-hoc transforms that “undo” schema-defined mappings, you should treat SchemaURL as untrustworthy (and may need to drop/clear it depending on pipeline semantics). ([GitHub][9])

---

## G9) Codegen + language packaging: eliminating stringly-typed SemConv usage

SemConv code can be auto-generated; languages may ship a stable subset or a preview superset. ([OpenTelemetry][10])
Python-specific packaging note: unstable attributes are exposed under `opentelemetry.semconv._incubating` (underscore indicates “internal/unstable”). ([OpenTelemetry][10])

**Governance pattern:** wrap SemConv keys behind a tiny internal “semconv facade” module, so changing keys becomes a single-point diff when SemConv shifts.

---

## G10) Events are LogRecords; event name is schema key (low-cardinality contract)

Standalone “Events” are represented as `LogRecord`s conforming to SemConv; an event **must** have an event name, which should be low-cardinality and uniquely defines the event’s structure (attrs + body type). ([OpenTelemetry][11])
Governance: never bake user IDs / request IDs into event names; keep names stable and put specifics in attributes/body.

---

## G11) Metric SemConv governance: hierarchy + discoverability

Metric SemConv recommends nesting metrics in hierarchies (OS cpu/network, runtime GC, etc.) to support discovery and ad-hoc comparison. ([OpenTelemetry][12])
This interacts directly with:

* View-based renames in SDKs
* Collector-side metric transforms
* Schema file `rename_metrics` rules ([OpenTelemetry][8])

---

## G12) SemConv contains explicit “sensitive info” rules (URL is the canonical example)

* `url.full` must not include credentials; if present, username/password should be redacted. ([OpenTelemetry][13])
* URL conventions explicitly warn about security risk: user/password must not be recorded; sensitive query params must be scrubbed; consumers may need to redact entirely if they can’t identify sensitivity. ([OpenTelemetry][14])

This is where SemConv governance meets security governance: “semantic correctness” includes “don’t standardize exfiltration.”

---

# H) Security, privacy, and data governance “advanced” topics

## Mental model (end-to-end responsibility + defense in depth)

OpenTelemetry can’t infer what’s sensitive for your org; implementers are responsible for compliance, consent, and reviewing what instrumentation emits. ([OpenTelemetry][15])
Security governance is layered:

1. **Don’t collect** (minimization)
2. **Scrub at source** (SDK/instrumentation config)
3. **Scrub in pipeline** (Collector processors)
4. **Secure transport + endpoints** (TLS/mTLS/auth, network binding)
5. **Secure storage + access** (backend RBAC, retention, encryption) ([OpenTelemetry][15])

---

## H1) Sensitive-data threat surface inventory (what actually leaks)

Common leak vectors explicitly called out in OTel security guidance:

* PII, credentials, session tokens, financial/health data, behavioral data. ([OpenTelemetry][15])
* **URLs**: user/password must not be recorded; sensitive query params must be scrubbed. ([OpenTelemetry][14])
* **Baggage**: is propagated across services and commonly sent in HTTP headers by auto-instrumentation; sensitive baggage can leak to third-party APIs (network-visible). ([OpenTelemetry][16])
* **Collector config/secrets**: API tokens, TLS private keys can live in config; treat the config file as sensitive. ([OpenTelemetry][17])

---

## H2) Data minimization as a *continuous control*

Principle: collect only what serves an observability purpose; avoid personal info unless necessary; prefer aggregated/anonymized; regularly review attributes. ([OpenTelemetry][15])
Operationally, this implies:

* “Attribute budget” reviews (what keys exist, who set them, why)
* Default-deny capture for high-risk surfaces (headers, query params, bodies)
* Strict conventions for tenant/user identifiers (hashing/truncation, never raw)

---

## H3) Collector-side scrub primitives (mechanical, policy-driven)

### H3.1 Attribute processor: delete/hash specific keys

OpenTelemetry’s handling-sensitive-data guide gives a canonical pattern: hash `user.email`, delete `user.full_name`. ([OpenTelemetry][15])

```yaml
processors:
  attributes/example:
    actions:
      - key: user.email
        action: hash
      - key: user.full_name
        action: delete
```

### H3.2 Transform processor (OTTL): computed anonymization + structural rewrites

Replace `user.id` with `user.hash` (SHA256 + delete), and truncate IPv4 last octet: ([OpenTelemetry][15])

```yaml
processors:
  transform:
    trace_statements:
      - context: span
        statements:
          - set(attributes["user.hash"], SHA256(attributes["user.id"]))
          - delete_key(attributes, "user.id")
          - replace_pattern(attributes["client.address"], "\\.\\d+$", ".0")
```

Note: the same doc cautions hashing may be reversible if the input space is small/predictable (e.g., sequential numeric IDs). ([OpenTelemetry][15])

### H3.3 Redaction processor: allowlist keys + block patterns on values

Collector config best practices describe redaction as: remove attributes not in allowed list, then mask blocked values on remaining allowed keys. ([OpenTelemetry][17])

```yaml
processors:
  redaction:
    allow_all_keys: false
    allowed_keys: [description, group, id, name]
    blocked_values:
      - '4[0-9]{12}(?:[0-9]{3})?'   # Visa-like pattern
      - '(5[1-5][0-9]{14})'         # MasterCard-like pattern
```

(Also supports `ignored_keys` and summaries.) ([OpenTelemetry][17])

### H3.4 Filter processor: drop whole telemetry items

Security guidance explicitly recommends filtering spans/metrics containing sensitive data; config best practices show filter used to drop unwanted spans (example: drop spans missing HTTP method). ([OpenTelemetry][15])

### H3.5 Governance framing

Collector is explicitly positioned as a convenient place to transform telemetry for **data quality, governance, cost, and security** reasons. ([OpenTelemetry][18])

---

## H4) Securing the Collector itself (endpoints, config, permissions)

### H4.1 Config secret management

Collector config can contain API tokens and TLS private keys; store on encrypted FS/secret store; leverage env-var expansion to avoid hardcoding secrets. ([OpenTelemetry][17])

### H4.2 Reduce attack surface (binary + config)

Minimize components to what you use (attack surface reduction); build custom distributions if needed. ([OpenTelemetry][17])

### H4.3 Least privilege + “server-like component” control

* Avoid running as root; apply least privilege. ([OpenTelemetry][17])
* Receivers/exporters can be server-like; limit access by enabling authentication extensions and restricting IPs/interfaces. ([OpenTelemetry][19])

### H4.4 DoS posture: bind narrowly (localhost/pod IP)

Security best practices recommend binding endpoints to limited interfaces (localhost/pod IP) instead of `0.0.0.0`; Collector’s defaults have moved toward localhost in recent versions and can be forced via feature gate in earlier versions. ([OpenTelemetry][17])

---

## H5) Transport security: OTLP channel guarantees + exporter/receiver expectations

* OTLP is stable for traces/metrics/logs; supports gRPC and HTTP transports; supports `none` and `gzip` compression options. ([OpenTelemetry][20])
* OTLP exporter spec: endpoint URL scheme `https` indicates secure connection; per-signal endpoints override base endpoint behavior. ([OpenTelemetry][21])
* In practice: enforce TLS/mTLS between SDKs/agents/collectors/backends; treat “insecure dev flags” as production-prohibited.

---

## H6) Baggage governance: the “silent exfil” vector

Baggage is propagated across services/processes and is often automatically injected into network requests; sensitive baggage can be shared with unintended resources because it travels in headers and can be forwarded outside your network. ([OpenTelemetry][16])
Governance rules:

* Never put secrets/tokens in baggage.
* Treat baggage keys as “public metadata” unless proven otherwise.
* If you need user/tenant identifiers, prefer hashed/truncated forms and explicitly add them as attributes only where required.

---

## H7) URL governance: SemConv mandates scrubbing

URL SemConv explicitly forbids recording user/password and requires scrubbing known-sensitive query params; where you can’t reliably identify sensitivity, redaction may be necessary. ([OpenTelemetry][14])
This should become an org-level policy: “URL attributes are sanitized at source or in Collector, always.”

---

## H8) Putting it together: “secure telemetry pipeline” minimal blueprint

1. **Minimize capture** (no raw IDs/headers/bodies by default) ([OpenTelemetry][15])
2. **Normalize SemConv** (schema processor early) so downstream rules don’t break on drift ([Go Packages][3])
3. **Scrub** (attributes/transform/redaction/filter processors) ([OpenTelemetry][15])
4. **Secure Collector hosting** (secrets, least privilege, restricted binds) ([OpenTelemetry][17])
5. **Secure transport** (TLS/mTLS/auth) and monitor Collector’s own telemetry for drops/backpressure (resource + security) ([OpenTelemetry][17])

[1]: https://opentelemetry.io/docs/concepts/semantic-conventions/ "Semantic Conventions | OpenTelemetry"
[2]: https://opentelemetry.io/docs/specs/otel/schemas/ "Telemetry Schemas | OpenTelemetry"
[3]: https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor "schemaprocessor package - github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor - Go Packages"
[4]: https://opentelemetry.io/docs/specs/semconv/ "OpenTelemetry semantic conventions 1.38.0 | OpenTelemetry"
[5]: https://opentelemetry.io/docs/specs/otel/semantic-conventions/ "Semantic Conventions | OpenTelemetry"
[6]: https://opentelemetry.io/docs/specs/semconv/general/semantic-convention-groups/ "Semantic convention groups | OpenTelemetry"
[7]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/ "Semantic conventions for HTTP metrics | OpenTelemetry"
[8]: https://opentelemetry.io/docs/specs/otel/schemas/file_format_v1.0.0/ "Schema File Format 1.0.0 | OpenTelemetry"
[9]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/19472?utm_source=chatgpt.com "Processors should remove SchemaURL when ..."
[10]: https://opentelemetry.io/docs/specs/semconv/non-normative/code-generation/ "Generating semantic convention libraries | OpenTelemetry"
[11]: https://opentelemetry.io/docs/specs/semconv/general/events/ "Semantic conventions for events | OpenTelemetry"
[12]: https://opentelemetry.io/docs/specs/semconv/general/metrics/ "Metrics semantic conventions | OpenTelemetry"
[13]: https://opentelemetry.io/docs/specs/semconv/registry/attributes/url/ "URL | OpenTelemetry"
[14]: https://opentelemetry.io/docs/specs/semconv/url/ "Semantic conventions for URL | OpenTelemetry"
[15]: https://opentelemetry.io/docs/security/handling-sensitive-data/ "Handling sensitive data | OpenTelemetry"
[16]: https://opentelemetry.io/docs/concepts/signals/baggage/ "Baggage | OpenTelemetry"
[17]: https://opentelemetry.io/docs/security/config-best-practices/ "Collector configuration best practices | OpenTelemetry"
[18]: https://opentelemetry.io/docs/collector/transforming-telemetry/ "Transforming telemetry | OpenTelemetry"
[19]: https://opentelemetry.io/docs/security/hosting-best-practices/ "Collector hosting best practices | OpenTelemetry"
[20]: https://opentelemetry.io/docs/specs/otlp/ "OTLP Specification 1.9.0 | OpenTelemetry"
[21]: https://opentelemetry.io/docs/specs/otel/protocol/exporter/ "OpenTelemetry Protocol Exporter | OpenTelemetry"

# I) Testing, troubleshooting, and “debuggability at scale”

## Mental model (why OTel is “hard to test”)

Telemetry pipelines are **asynchronous + stateful + multi-stage**:

* **SDK stage**: spans/logs/metrics are buffered (Batch processors, periodic readers) and only become externally visible on **flush/export cadence**. ([OpenTelemetry Python][1])
* **Collector stage**: receiver→processor→exporter adds buffering/backpressure and can drop/transform data; you debug by **hop-by-hop validation**. ([OpenTelemetry][2])
* **Scale stage**: correctness depends on process model (fork/reload), queue sizing, and “control plane drift” (config/env var mismatches). ([OpenTelemetry][3])

Design target for tests/debug: make each stage **observable in isolation** (in-memory / console / debug exporter), then re-compose.

---

## I1) Unit-test harnesses (SDK-only): deterministic capture by signal

### I1.1 Traces: “end-span → capture → assert” without external IO

**Primitives**

* `ConsoleSpanExporter` exists explicitly for diagnostic output to stdout. ([OpenTelemetry Python][1])
* `SpanExporter` contract requires wiring through `SimpleSpanProcessor` or `BatchSpanProcessor`; processor callbacks are synchronous on start/end (don’t block). ([OpenTelemetry Python][1])
* `BatchSpanProcessor` is env-var configurable (`OTEL_BSP_*`) and uses background work; tests should avoid depending on its timing unless you explicitly `force_flush()`. ([OpenTelemetry Python][1])
* `InMemorySpanExporter` exists for testing and stores exported spans in-memory retrievable via `get_finished_spans`. ([GitHub][4])

**Fixture pattern (pytest)**

```python
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

@pytest.fixture
def span_capture():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    try:
        yield exporter
    finally:
        provider.shutdown()  # drains processors/exporters
        exporter.clear()     # avoid cross-test leakage (method exists on exporter)
```

(Exporter lifecycle correctness depends on flush/shutdown; `SpanExporter.force_flush()`/`shutdown()` are first-class. ([OpenTelemetry Python][1]))

**Assertion discipline**

* Assert against **ended spans** (captured by exporter), not “current span” references, unless you control span end synchronously. (`start_as_current_span` ends on context manager exit.) ([OpenTelemetry Python][5])
* If you *must* assert against “active spans” (async end elsewhere), treat it as a distinct test style: you’re validating **context propagation**, not exporter output (and this is a known pain-point in real projects). ([GitHub][6])

---

### I1.2 Metrics: never rely on background periodic collection in unit tests

**Primitives**

* `InMemoryMetricReader.get_metrics_data()` is explicitly “useful for unit tests.” ([OpenTelemetry Python][7])
* `ConsoleMetricExporter` exists for diagnostics (stdout). ([OpenTelemetry Python][7])
* Auto-instrumentation + pre-fork servers can break metrics generation because `PeriodicExportingMetricReader` spawns a thread; forked workers inherit inconsistent locks/threads. ([OpenTelemetry][3])

**Fixture pattern**

```python
import pytest
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

@pytest.fixture
def metric_capture():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)
    try:
        yield reader
    finally:
        provider.shutdown()
```

**Assertion discipline**

* Pull via `reader.get_metrics_data()` at a deterministic point; avoid “sleep until exported”. ([OpenTelemetry Python][7])
* When your production pipeline uses delta temporality / special aggregations, mirror those via reader/exporter “preferred temporality/aggregation” so your unit test output matches production semantics. ([OpenTelemetry Python][7])

---

### I1.3 Logs: test the *OTel logs pipeline*, not the stdlib formatter

**Primitives**

* Python “manual instrumentation” docs show wiring `LoggerProvider` + `BatchLogRecordProcessor` + `ConsoleLogRecordExporter` (note naming drift: older versions had `ConsoleLogExporter`). ([OpenTelemetry][8])
* Logs are still flagged as development/experimental in Python-facing docs; treat the types and import paths as drift-prone and pin versions aggressively. ([OpenTelemetry Python][9])
* Stdout exporters have **unspecified output format** (don’t snapshot raw text unless you control formatter). ([OpenTelemetry][10])

**Fixture pattern**

```python
import logging, pytest
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogRecordExporter

@pytest.fixture
def otel_log_pipeline():
    provider = LoggerProvider()
    set_logger_provider(provider)
    provider.add_log_record_processor(BatchLogRecordProcessor(ConsoleLogRecordExporter()))
    handler = LoggingHandler(logger_provider=provider)

    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield
    finally:
        provider.shutdown()
        root.removeHandler(handler)
```

(Use this to validate “OTel sees my log records” independent of how your app formats logs.) ([OpenTelemetry][8])

---

## I2) “Debug exporters” as a systematic bisect tool (SDK + Collector)

### I2.1 SDK console exporters (fastest local truth)

* Python docs explicitly recommend console exporters “to debug your instrumentation” and show `ConsoleSpanExporter`/`ConsoleMetricExporter`. ([OpenTelemetry][11])
* `ConsoleSpanExporter`/`ConsoleMetricExporter` are explicitly “diagnostic purposes.” ([OpenTelemetry Python][1])

### I2.2 Agent-level: console + OTLP side-by-side

Zero-code doc supports multi-exporter (e.g., `--traces_exporter console,otlp`) and env-based equivalents. ([OpenTelemetry][12])
This is the “least-effort bisect”: if console shows data but OTLP doesn’t, your failure is **egress** not **instrumentation**.

---

### I2.3 Collector “debug exporter” (the canonical hop validator)

Collector troubleshooting docs recommend using the **debug exporter** to confirm receive/process/export and show `verbosity: detailed` printing full payloads. ([OpenTelemetry][2])
The Python distro docs also provide a local collector config using `debug` exporter (and note the legacy name change: older Collector versions used `logging` exporter instead of `debug`). ([OpenTelemetry][13])

---

## I3) Troubleshooting workflow: pipeline bisection (mechanical checklist)

### I3.1 Stage 0 — verify the agent + distro wiring exists

* Auto-instrumentation requires installing a distro package (`opentelemetry-distro` is the default). ([OpenTelemetry][12])

### I3.2 Stage 1 — prove the SDK is emitting

* Switch to console exporters (stdout): `ConsoleSpanExporter` / `ConsoleMetricExporter`. ([OpenTelemetry][11])
* If using the agent: configure console exporters via CLI or env (doc shows both). ([OpenTelemetry][12])

### I3.3 Stage 2 — prove OTLP transport to Collector

* Run a minimal Collector that just receives OTLP and exports to debug. ([OpenTelemetry][13])
* Ensure endpoint/path semantics are correct (especially OTLP/HTTP `/v1/*` path handling); if unsure, force per-signal endpoints.

### I3.4 Stage 3 — prove Collector topology and component availability

Collector troubleshooting gives the exact “topology hazards” and tools:

* A receiver defined but not referenced in `service.pipelines` means “Collector not receiving data.” ([OpenTelemetry][2])
* `otelcol components` lists which receivers/processors/exporters exist in your distribution and their stability. ([OpenTelemetry][2])
* Enable extensions for deep debugging:

  * `pprof` (profiling; port 1777) ([OpenTelemetry][2])
  * `zpages` (live receiver/exporter inspection; TraceZ shows running spans that don’t end, latency issues, errors; configurable endpoint). ([OpenTelemetry][2])

### I3.5 Stage 4 — confirm “scale failure modes”

Collector troubleshooting enumerates common root causes:

* Drops often caused by undersizing or slow/unavailable exporter destination; mitigate via exporter queued retry options / sending queue batch settings. ([OpenTelemetry][2])

---

## I4) High-frequency Python failure modes you should explicitly document (because they look like “OTel is broken”)

### I4.1 Dev servers / reloaders

Flask debug mode enables a reloader; the OTel troubleshooting guide notes this can break instrumentation unless `use_reloader=False`. ([OpenTelemetry][3])

### I4.2 Pre-fork servers (Gunicorn, multi-worker) + metrics

Python troubleshooting notes multi-worker pre-fork setups can break metrics due to `PeriodicExportingMetricReader` spawning threads and forking causing inconsistent thread/lock state; issues are tracked upstream. ([OpenTelemetry][3])
**Takeaway for tests/bench**: prefer `InMemoryMetricReader` and explicit `shutdown()`/collection in unit tests; treat periodic readers as integration/runtime-only. ([OpenTelemetry][3])

### I4.3 Output format instability

Stdout exporters’ output format is explicitly unspecified and can vary; don’t write “golden file” snapshots over raw console exporter output without a stable formatter layer. ([OpenTelemetry][10])

---

## I5) Debuggability at scale: what to monitor when “some telemetry disappears”

At scale, you don’t ask “did we export?”—you ask “where did it stop?”:

* **Collector internal telemetry**: first-line signal for backpressure, queue growth, exporter errors. ([OpenTelemetry][2])
* **Debug exporter sampling**: send a small known payload through and confirm it survives each hop (Collector docs include a Zipkin payload injection example and expected debug logs). ([OpenTelemetry][2])
* **Hop checklist**: Collector docs provide a concrete “verify ingestion / modification / export / format / next hop / network policy” checklist—use this as your standard incident runbook skeleton. ([OpenTelemetry][2])

---

# J) Distro / vendor distribution ecosystem (optional but real-world important)

## Mental model (distribution ≠ fork; distros are “opinionated wrappers”)

OpenTelemetry defines a **distribution** as a **customized wrapper** around an upstream OTel component (language libs or Collector), explicitly “not a fork.” ([OpenTelemetry][14])
Allowed customization classes include: scripts, default settings, packaging options, extra test/security coverage, added capabilities, or removed capabilities. ([OpenTelemetry][14])

OTel categorizes distributions:

* **Pure**: same functionality as upstream, just easier packaging/defaults
* **Plus**: adds capabilities beyond upstream (extra exporters/instrumentations/features)
* **Minus**: removes upstream capabilities (supportability/security surface reduction) ([OpenTelemetry][14])

Governance disclaimers are explicit:

* OTel does not validate/endorse third-party distros; support comes from distro authors; evaluate vendor lock-in. ([OpenTelemetry][14])

---

## J1) Upstream Python distro mechanics (what the default `opentelemetry-distro` actually does)

The Python distro docs state `opentelemetry-distro` configures (by default):

* SDK `TracerProvider`
* `BatchSpanProcessor`
* OTLP `SpanExporter` to send to an OTel Collector ([OpenTelemetry][13])

It also defines the extensibility mechanism:

* auto-instrumentation loads distro/configuration interfaces via **entry points** `opentelemetry_distro` and `opentelemetry_configurator`, executed **before any other code**. ([OpenTelemetry][13])
  And zero-code docs emphasize: you must install a distro package for auto-instrumentation to work. ([OpenTelemetry][12])

---

## J2) Selecting/customizing Python distros in-process (entry point discovery)

The `opentelemetry-instrumentation` packaging guidance notes:

* if multiple distros/configurators are present, select via `OTEL_PYTHON_DISTRO` and `OTEL_PYTHON_CONFIGURATOR`. ([PyPI][15])

**Environment introspection snippet (no magic):**

```python
import importlib.metadata as md

print([ep.name for ep in md.entry_points(group="opentelemetry_distro")])
print([ep.name for ep in md.entry_points(group="opentelemetry_configurator")])
```

(Useful when an agent runtime “mysteriously” sets defaults you didn’t configure.)

---

## J3) Third-party distro ecosystem: where to look + what “vendor distro” typically changes

OpenTelemetry maintains an ecosystem list of third-party distributions and the component they customize (including Python distros like AWS ADOT). ([OpenTelemetry][16])

Typical “vendor distro” customizations you should catalog (because they affect debugging and migrations):

* **Default exporter + endpoint + protocol** (often OTLP/gRPC to a local vendor Collector)
* **Default propagators** (W3C vs B3, etc.)
* **Resource enrichment** (cloud/platform metadata)
* **Additional instrumentation / hooks** (response header injection, log correlation)
* **Support policy and release cadence** (vendor may ship fixes before upstream) ([Splunk Docs][17])

Concrete Python examples (representative):

### Splunk Distribution of OpenTelemetry Python

Splunk describes its distro as a wrapper around upstream Python auto-instrumentation, adds features (server trace info in HTTP responses, automatic log trace-metadata injection), and sets defaults including OTLP gRPC exporter to a local Splunk OTel Collector. ([Splunk Docs][17])

### AWS Distro for OpenTelemetry (ADOT) Python

AWS describes ADOT Python as enabling instrumentation once and sending correlated telemetry to AWS monitoring solutions, requiring both an OTel SDK configured for X-Ray and an ADOT Collector configured for X-Ray. ([AWS Documentation][18])

### Azure Monitor OpenTelemetry Distro (Python)

Microsoft’s package description emphasizes bundling OTel + Azure Monitor components and providing a `configure_azure_monitor` entry point; docs note automatic instrumentation is not yet supported (so it’s “manual-first”). ([Microsoft Learn][19])

### Lumigo OpenTelemetry Distribution for Python (example of “plus distro”)

Lumigo describes a no-code distro built from upstream packages with additional customizations and optional log correlation support. ([GitHub][20])

---

## J4) Collector distributions (vendor vs custom builds)

Distributions apply equally to the Collector binary. OTel’s distro concept page explicitly calls out Collector binaries as reference implementations that can be customized. ([OpenTelemetry][14])
The ecosystem distro list distinguishes non-collector distros vs Collector distributions (separate lists). ([OpenTelemetry][16])

Operational implication: “Collector behavior” depends on **which distribution** you run (component set, defaults, enabled extensions).

---

## J5) Evaluation matrix for choosing a distro (what advanced agents must verify)

Because OTel does not certify distros and support is from authors: ([OpenTelemetry][14])
Verify (mechanically) before committing:

* Component coverage: which signals are stable/beta in that distro (logs often differ)
* Defaults: exporters/protocols/endpoints/sampling/propagators
* Added behaviors: response header injection, log correlation, secret masking
* Escape hatches: can you revert to upstream config knobs (`OTEL_*`) cleanly?
* Lock-in surface: do you emit vendor-specific attributes, or can you run through a generic Collector?

---

If you want the next continuation after I/J, the natural remaining “advanced backlog” chapters are: **K) Cross-signal correlation patterns (trace↔metrics exemplars, logs trace-based gating, baggage governance)** and **L) Performance/cost engineering (cardinality budgets, sampling + tail-sampling topology, queue sizing, and failure-mode testing)**—but I’ll stop here per your request.

[1]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.export.html "opentelemetry.sdk.trace.export — OpenTelemetry Python  documentation"
[2]: https://opentelemetry.io/docs/collector/troubleshooting/ "Troubleshooting | OpenTelemetry"
[3]: https://opentelemetry.io/docs/zero-code/python/troubleshooting/ "Troubleshooting Python automatic instrumentation issues | OpenTelemetry"
[4]: https://github.com/open-telemetry/opentelemetry-python/blob/main/opentelemetry-sdk/src/opentelemetry/sdk/trace/export/in_memory_span_exporter.py?utm_source=chatgpt.com "open-telemetry/opentelemetry-python"
[5]: https://opentelemetry-python.readthedocs.io/en/latest/api/trace.html?utm_source=chatgpt.com "opentelemetry.trace package"
[6]: https://github.com/open-telemetry/opentelemetry-python/issues/3999?utm_source=chatgpt.com "trace.get_current_span doesn't provide active span under ..."
[7]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.export.html "opentelemetry.sdk.metrics.export — OpenTelemetry Python  documentation"
[8]: https://opentelemetry.io/docs/languages/python/instrumentation/?utm_source=chatgpt.com "Instrumentation"
[9]: https://opentelemetry-python.readthedocs.io/en/stable/examples/logs/README.html?utm_source=chatgpt.com "OpenTelemetry Logs SDK"
[10]: https://opentelemetry.io/docs/specs/otel/logs/sdk_exporters/stdout/?utm_source=chatgpt.com "Logs Exporter - Standard output"
[11]: https://opentelemetry.io/docs/languages/python/exporters/?utm_source=chatgpt.com "Exporters"
[12]: https://opentelemetry.io/docs/zero-code/python/ "Python zero-code instrumentation | OpenTelemetry"
[13]: https://opentelemetry.io/docs/languages/python/distro/ "OpenTelemetry Distro | OpenTelemetry"
[14]: https://opentelemetry.io/docs/concepts/distributions/ "Distributions | OpenTelemetry"
[15]: https://pypi.org/project/opentelemetry-instrumentation/?utm_source=chatgpt.com "opentelemetry-instrumentation"
[16]: https://opentelemetry.io/ecosystem/distributions/ "Third-party distributions | OpenTelemetry"
[17]: https://help.splunk.com/en/splunk-observability-cloud/manage-data/available-data-sources/supported-integrations-in-splunk-observability-cloud/apm-instrumentation/instrument-a-python-application/about-splunk-otel-python?utm_source=chatgpt.com "About the Splunk Distribution of OpenTelemetry Python"
[18]: https://docs.aws.amazon.com/xray/latest/devguide/xray-python-opentel-sdk.html?utm_source=chatgpt.com "AWS Distro for OpenTelemetry Python - AWS X-Ray"
[19]: https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-opentelemetry-readme?view=azure-python&utm_source=chatgpt.com "Azure Monitor Opentelemetry Distro client library for Python"
[20]: https://github.com/lumigo-io/opentelemetry-python-distro?utm_source=chatgpt.com "lumigo-io/opentelemetry-python-distro"
