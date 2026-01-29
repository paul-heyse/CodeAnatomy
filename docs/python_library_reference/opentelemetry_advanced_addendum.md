Below is a dense “core SDK” deep dive focused strictly on:

* `opentelemetry-api`
* `opentelemetry-sdk`
* `opentelemetry-semantic-conventions`

…and how they actually fit together in a production-grade Python telemetry stack.

---

## Mental model: API vs SDK vs SemConv

**Hard separation is intentional:**

* **`opentelemetry-api`** = *interfaces + global accessors + no-op fallbacks*. Libraries should depend only on this, so they don’t force an SDK choice on applications. ([GitHub][1])
* **`opentelemetry-sdk`** = *reference implementation* of those interfaces: processors, samplers, exporters wiring, metric readers, resource association, shutdown/flush, limits, etc. ([GitHub][1])
* **`opentelemetry-semantic-conventions`** = *generated constants* for standardized attribute keys / names defined by the spec. ([PyPI][2])

The spec also requires that **in the absence of an installed/configured SDK, the API behaves as a no-op** (but still supports context propagation semantics where required). ([OpenTelemetry][3])

---

## 1) `opentelemetry-api` (interfaces + globals + no-op)

### 1.1 Package contents and “no-op by default” guarantees

The Python API package includes:

* Traces API (`opentelemetry.trace`)
* Metrics API (`opentelemetry.metrics`)
* Logs API (`opentelemetry._logs`)
* Context API (`opentelemetry.context`)
* Propagation API (`opentelemetry.propagate`)
* Environment variable constants and configuration hooks (`opentelemetry.environment_variables`, etc.) ([OpenTelemetry Python][4])

**No-op behavior is not an accident; it’s required.**

* Trace API spec: without SDK, trace API is a no-op (with special handling so propagated `SpanContext` can continue to flow). ([OpenTelemetry][3])
* Metrics has a defined no-op API implementation requirement. ([OpenTelemetry][5])
* Baggage API must be fully functional without an SDK to support cross-process propagation. ([OpenTelemetry][6])
* Python logs API explicitly provides a `NoOpLogger`/`NoOpLoggerProvider`. ([OpenTelemetry Python][7])

This is why *instrumentation libraries should only depend on `opentelemetry-api`* (and optionally semconv), and leave SDK configuration to the application. ([OpenTelemetry][8])

---

### 1.2 Global providers, “instrumentation scope”, and naming rules

Each signal has a **Provider** as the entry point, with global getters/setters:

* Tracing: `trace.get_tracer_provider()`, `trace.set_tracer_provider()`, `trace.get_tracer(...)` ([OpenTelemetry Python][9])
* Metrics: `metrics.get_meter_provider()`, `metrics.set_meter_provider()`, `metrics.get_meter(...)` ([OpenTelemetry Python][10])
* Logs: `_logs.get_logger_provider()`, `_logs.set_logger_provider()`, `_logs.get_logger(...)` ([OpenTelemetry Python][7])

**Instrumentation scope** is *how telemetry identifies who emitted it* (instrumentation library / module / component):

* Spec: scope name + optional version + optional schema URL + optional scope attributes. ([OpenTelemetry][11])
* Python trace API explicitly says: **don’t use `__name__`** if that would vary per file; prefer a fixed, importable string used consistently. ([OpenTelemetry Python][9])
* Python metrics SDK docs similarly warn that `__name__` may cause different meter names across files. ([OpenTelemetry Python][12])

**LLM-agent rule of thumb:**
Define one canonical scope string per subsystem, e.g.:

* `"codeanatomy.build"`
* `"codeanatomy.http"`
* `"codeanatomy.extractors.tree_sitter"`
  …and pass versions/schema URLs only when you truly control them.

---

### 1.3 Context: the execution-local state carrier

OpenTelemetry’s “cross-cutting concerns” (trace context, baggage, suppression flags, etc.) sit in a **Context** that flows with execution.

Python API exposes:

* `get_current()`, `attach(ctx) -> token`, `detach(token)` ([OpenTelemetry Python][13])
* `create_key(...)`, `set_value(key, value, context=...)`, `get_value(...)` ([OpenTelemetry Python][14])

#### RuntimeContext implementation selection (`OTEL_PYTHON_CONTEXT`)

Python chooses a runtime context implementation via entry points:

* default is `"contextvars_context"` and it is selected via env var `OTEL_PYTHON_CONTEXT` (group `"opentelemetry_context"`). ([OpenTelemetry Python][14])

This aligns with Python’s `contextvars`, which is designed for context-local state across async tasks and threads. ([Python documentation][15])

**Practical implication for your codebase:**
If you mix threads, asyncio, subprocesses, and “manual context passing”, treat `attach/detach` as the “escape hatch” for explicit context boundaries; otherwise rely on contextvars-driven implicit propagation.

---

### 1.4 Propagation: injecting/extracting context across process boundaries

The API requires a way to get/set global propagators; default should be a composite of W3C Trace Context + W3C Baggage (where preconfigured). ([OpenTelemetry][16])

Python’s `opentelemetry.propagate`:

* builds a composite propagator based on `OTEL_PROPAGATORS`
* default is `tracecontext,baggage` ([OpenTelemetry Python][17])

**Baggage** is explicitly defined as request/workflow-scoped key/value pairs that can annotate traces/metrics/logs, and it is designed to propagate without requiring an SDK. ([OpenTelemetry][6])

---

### 1.5 Trace API: the contract the SDK must implement

Key types and behaviors:

* `TracerProvider` → `Tracer` → creates `Span`s ([OpenTelemetry][3])
* `SpanContext` carries `trace_id`, `span_id`, `TraceFlags` (sampled bit), `TraceState`, and `is_remote`. ([OpenTelemetry][3])
* Span creation rules (parentage is derived from `Context`; spans can be “current” or detached). ([OpenTelemetry][3])
* Python `start_span(...)` includes `record_exception` and `set_status_on_exception` behavior when used as a context manager. ([OpenTelemetry Python][9])

**Agent-level performance tip:**
When doing expensive attribute computation, gate on “is recording” (span API supports `is_recording()`), because no-op and non-recording spans should avoid overhead. (The spec’s “enabled” fast-path is discussed as an API feature; implementations may expose it). ([OpenTelemetry][3])

---

### 1.6 Metrics API: instruments without committing to export mechanics

The API defines `MeterProvider` → `Meter` → instruments; also has a `NoOpMeter` so libraries can call it safely without an SDK. ([OpenTelemetry Python][10])

Exporting, temporality, aggregation, readers, etc. are **SDK concerns**, not API concerns.

---

### 1.7 Logs API (`opentelemetry._logs`): “development” signal surface, still structured

Python exposes:

* `LoggerProvider` / `Logger`
* `Logger.emit(...)` producing a `LogRecord`
* no-op logger/provider when unconfigured ([OpenTelemetry Python][7])

Note: The Python project has historically treated logs as less mature than traces/metrics, but the API surface exists and the spec defines a logs SDK. ([OpenTelemetry][18])

---

## 2) `opentelemetry-sdk` (reference implementation: pipelines + limits + shutdown)

The SDK is where telemetry becomes *real*: spans become `ReadableSpan`s, metrics become exported streams, logs become exported records.

### 2.1 Resource: immutable identity of the emitting entity

A `Resource` is immutable and attached at provider construction time; it’s how backends know “which service/pod/process emitted this”. ([OpenTelemetry][19])

Python:

* create via `Resource.create({...})`
* can populate via env `OTEL_RESOURCE_ATTRIBUTES`
* pass to `TracerProvider` / `MeterProvider` constructors ([OpenTelemetry Python][20])

Conceptually:

* `service.name` should be set explicitly (default otherwise is `unknown_service`) and can be set via `OTEL_SERVICE_NAME`. ([OpenTelemetry][21])

---

### 2.2 Tracing SDK (`opentelemetry.sdk.trace`)

#### Provider construction = your “trace pipeline root”

`TracerProvider(...)` accepts:

* `sampler`
* `resource`
* `id_generator`
* `span_limits`
* `active_span_processor`
* `shutdown_on_exit` (atexit-driven shutdown) ([OpenTelemetry Python][22])

#### Span creation pipeline (spec-mandated order)

When creating a span, the SDK must:

1. reuse parent trace id if valid or generate new
2. call `Sampler.should_sample`
3. generate span id regardless of sampling outcome
4. create either recording or non-recording span accordingly ([OpenTelemetry][23])

This matters because **even unsampled/non-recording spans still need stable IDs** for correlation with logs/exceptions in-process.

#### Sampling

Python SDK supports configuring sampler via env vars:

* `OTEL_TRACES_SAMPLER`
* `OTEL_TRACES_SAMPLER_ARG` ([OpenTelemetry Python][24])

Common built-ins include ratio-based sampling (e.g., `TraceIdRatioBased`). ([OpenTelemetry Python][24])

#### SpanProcessors

`SpanProcessor` hooks:

* `on_start(span, parent_context)`
* `on_end(readable_span)`
* `force_flush(...)`
* `shutdown()` ([OpenTelemetry Python][22])

Critical detail: `on_start` / `on_end` are called **synchronously** on the thread ending/starting the span; they must not block. ([OpenTelemetry Python][22])

#### Export path: Simple vs Batch

SDK provides:

* `SpanExporter` interface
* `SimpleSpanProcessor` (exports synchronously on end)
* `BatchSpanProcessor` (queues and exports in batches) ([OpenTelemetry Python][25])

BatchSpanProcessor is controlled by env vars (spec + python implementation):

* `OTEL_BSP_SCHEDULE_DELAY`
* `OTEL_BSP_MAX_QUEUE_SIZE`
* `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`
* `OTEL_BSP_EXPORT_TIMEOUT` ([OpenTelemetry Python][25])

**Agent rule:** default to BatchSpanProcessor unless you have a reason not to.

#### Limits (attribute/event/link truncation & dropping)

Python exposes `SpanLimits` which reads:

* defaults
* env vars
* explicit overrides ([OpenTelemetry Python][22])

Spec defines default limits (commonly 128) and requires SDKs to log discards at most once per span to avoid log spam. ([OpenTelemetry][23])

#### ID generation

Spec: SDK must generate random trace/span IDs by default and allow customization; vendor-specific protocols must not be shipped in core repos. ([OpenTelemetry][23])

Python adds selection hooks via env vars like `OTEL_PYTHON_ID_GENERATOR`. ([OpenTelemetry Python][4])

---

### 2.3 Metrics SDK (`opentelemetry.sdk.metrics`)

#### Core components

* `MeterProvider(metric_readers=[...], resource=..., views=[...])` ([OpenTelemetry Python][12])
* Each `MetricReader` is independent and will collect separate metric streams. ([OpenTelemetry Python][12])

#### Views and Aggregations (this is where “metric shape” is controlled)

Python SDK supports Views:

* match instruments by name
* rename outputs
* change aggregation
* drop instruments via `DropAggregation` ([OpenTelemetry Python][12])

Spec describes Views as a first-class SDK mechanism, and defines “Aggregation” as the computation strategy (sum, histogram, etc.). ([OpenTelemetry][26])

**Agent pattern:** For “best-in-class” metrics hygiene, use Views to:

* drop noisy/undesired instruments by default
* explicitly re-enable the ones you care about (Python docs show a match-all drop + re-enable pattern). ([OpenTelemetry Python][12])

#### Periodic exporting configuration

Spec-defined env vars for periodic exporting metric readers:

* `OTEL_METRIC_EXPORT_INTERVAL`
* `OTEL_METRIC_EXPORT_TIMEOUT` ([OpenTelemetry][27])

Also exemplar filtering:

* `OTEL_METRICS_EXEMPLAR_FILTER` ([OpenTelemetry][27])

---

### 2.4 Logs SDK (`opentelemetry.sdk._logs`)

Python SDK provides:

* `LoggerProvider(resource=..., shutdown_on_exit=..., multi_log_record_processor=...)`
* `LogRecordProcessor` interface (`on_emit`, `shutdown`) ([OpenTelemetry Python][28])

Spec requires:

* Simple and Batch processors
* batching parameters similar to BSP ([OpenTelemetry][18])

Batch log processor env vars:

* `OTEL_BLRP_SCHEDULE_DELAY`
* `OTEL_BLRP_MAX_QUEUE_SIZE`
* `OTEL_BLRP_MAX_EXPORT_BATCH_SIZE`
* `OTEL_BLRP_EXPORT_TIMEOUT` ([OpenTelemetry][27])

---

### 2.5 SDK configuration via env vars (selection + defaults)

OpenTelemetry defines standard env var configuration across languages (traces/metrics/logs exporters, limits, batching, metric export intervals, etc.). ([OpenTelemetry][27])

Python exposes key variables (and some Python-specific ones) such as:

* `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER`
* `OTEL_PROPAGATORS`
* `OTEL_PYTHON_CONTEXT`, `OTEL_PYTHON_ID_GENERATOR`, `OTEL_PYTHON_TRACER_PROVIDER`, `OTEL_PYTHON_METER_PROVIDER` ([OpenTelemetry Python][4])

---

## 3) `opentelemetry-semantic-conventions` (generated constants for standardized keys)

### 3.1 What it is

PyPI: semantic conventions package contains **generated code** for semantic conventions defined by the OpenTelemetry specification. ([PyPI][2])

The spec: semantic conventions standardize attribute names, span names/kinds, metric instruments/units, etc., so telemetry is comparable across languages and libraries. ([OpenTelemetry][29])

### 3.2 What it looks like in Python

The OpenTelemetry Python manual instrumentation docs show direct use:

```python
from opentelemetry.semconv.trace import SpanAttributes
span.set_attribute(SpanAttributes.HTTP_METHOD, "GET")
span.set_attribute(SpanAttributes.HTTP_URL, "https://opentelemetry.io/")
```

([OpenTelemetry][8])

### 3.3 Stability, migrations, and why this is tricky in practice

Semantic conventions are **versioned** and can be stable/experimental/mixed. ([OpenTelemetry][29])

Spec guidance:

* Stable instrumentations **must not emit unstable conventions by default**, but may allow opt-in. ([OpenTelemetry][30])
* Many areas (HTTP, database, RPC, k8s) have explicit migration plans and an opt-in env var:
  `OTEL_SEMCONV_STABILITY_OPT_IN` with values like `http` / `http/dup`, `database` / `database/dup`, etc. ([OpenTelemetry][31])

**Why the env var exists:** instrumentations may have legacy attribute names (old conventions) and new stable names; “dup” modes allow emitting both during migration.

### 3.4 Schema URLs and “what version of semconv did this telemetry follow?”

OpenTelemetry has a formal Telemetry Schema mechanism:

* Telemetry includes a **schema URL** identifying the semantic convention schema version. ([OpenTelemetry][32])
* Instrumentation scope includes an optional schema URL, and APIs allow getting a Tracer/Meter associated with a schema URL. ([OpenTelemetry][11])

**Agent implication:** if you’re building internal instrumentation for CodeAnatomy subsystems, you can:

* keep using semconv constants for attributes
* optionally set `schema_url` consistently when you *want* schema-aware transformations downstream

…but do not invent schema URLs unless you’re prepared to publish/maintain transformations.

---

## Minimal “core-only” setup sketch (just to anchor the concepts)

This is *not* “best-in-class deployment” (that also needs OTLP exporters and/or distro/agent packages), but it shows how **API+SDK** connect:

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider

resource = Resource.create({"service.name": "codeanatomy"})

trace.set_tracer_provider(TracerProvider(resource=resource))
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(ConsoleSpanExporter())
)

metrics.set_meter_provider(MeterProvider(resource=resource))
```

Resource creation + env var support are core SDK concepts. ([OpenTelemetry Python][20])

---

## LLM-agent checklist for “core SDK” correctness

1. **Libraries depend on `opentelemetry-api` only**; apps configure `opentelemetry-sdk`. ([OpenTelemetry][8])
2. Use stable **instrumentation scope names** (avoid per-file `__name__` where it causes churn). ([OpenTelemetry Python][9])
3. Ensure context propagation uses the default contextvars runtime unless you have a specific reason to override `OTEL_PYTHON_CONTEXT`. ([OpenTelemetry Python][14])
4. Always set `service.name` via Resource (or `OTEL_SERVICE_NAME`), not ad-hoc attributes. ([OpenTelemetry][21])
5. Prefer Batch processors/readers; tune via standard env vars (`OTEL_BSP_*`, `OTEL_BLRP_*`, `OTEL_METRIC_EXPORT_*`). ([OpenTelemetry][27])
6. Use `opentelemetry-semantic-conventions` constants for attribute keys; be aware of stability migrations and `OTEL_SEMCONV_STABILITY_OPT_IN`. ([OpenTelemetry][8])


[1]: https://github.com/open-telemetry/opentelemetry-python?utm_source=chatgpt.com "GitHub - open-telemetry/opentelemetry-python: OpenTelemetry Python API and SDK"
[2]: https://pypi.org/project/opentelemetry-semantic-conventions/?utm_source=chatgpt.com "opentelemetry-semantic-conventions · PyPI"
[3]: https://opentelemetry.io/docs/specs/otel/trace/api/?utm_source=chatgpt.com "Tracing API | OpenTelemetry"
[4]: https://opentelemetry-python.readthedocs.io/en/stable/api/environment_variables.html?utm_source=chatgpt.com "opentelemetry.environment_variables package — OpenTelemetry Python documentation"
[5]: https://opentelemetry.io/docs/specs/otel/metrics/noop/?utm_source=chatgpt.com "Metrics No-Op API Implementation | OpenTelemetry"
[6]: https://opentelemetry.io/docs/specs/otel/baggage/api/?utm_source=chatgpt.com "Baggage API | OpenTelemetry"
[7]: https://opentelemetry-python.readthedocs.io/en/stable/api/_logs.html?utm_source=chatgpt.com "opentelemetry._logs package — OpenTelemetry Python documentation"
[8]: https://opentelemetry.io/docs/languages/python/instrumentation/?utm_source=chatgpt.com "Instrumentation | OpenTelemetry"
[9]: https://opentelemetry-python.readthedocs.io/en/latest/_modules/opentelemetry/trace.html?utm_source=chatgpt.com "opentelemetry.trace — OpenTelemetry Python documentation"
[10]: https://opentelemetry-python.readthedocs.io/en/stable/api/metrics.html?utm_source=chatgpt.com "opentelemetry.metrics package — OpenTelemetry Python documentation"
[11]: https://opentelemetry.io/docs/specs/otel/common/instrumentation-scope/?utm_source=chatgpt.com "Instrumentation Scope | OpenTelemetry"
[12]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.html?utm_source=chatgpt.com "opentelemetry.sdk.metrics package — OpenTelemetry Python documentation"
[13]: https://opentelemetry-python.readthedocs.io/en/stable/api/context.html?utm_source=chatgpt.com "opentelemetry.context package — OpenTelemetry Python documentation"
[14]: https://opentelemetry-python.readthedocs.io/en/stable/_modules/opentelemetry/context.html?utm_source=chatgpt.com "opentelemetry.context — OpenTelemetry Python documentation"
[15]: https://docs.python.org/3.9/library/contextvars.html?utm_source=chatgpt.com "contextvars — Context Variables — Python 3.9.23 documentation"
[16]: https://opentelemetry.io/docs/specs/otel/context/api-propagators/?utm_source=chatgpt.com "Propagators API | OpenTelemetry"
[17]: https://opentelemetry-python.readthedocs.io/en/latest/api/propagate.html?utm_source=chatgpt.com "opentelemetry.propagate package — OpenTelemetry Python documentation"
[18]: https://opentelemetry.io/docs/specs/otel/logs/sdk/?utm_source=chatgpt.com "Logs SDK | OpenTelemetry"
[19]: https://opentelemetry.io/docs/specs/otel/resource/sdk/?utm_source=chatgpt.com "Resource SDK | OpenTelemetry"
[20]: https://opentelemetry-python.readthedocs.io/en/latest/_modules/opentelemetry/sdk/resources.html?utm_source=chatgpt.com "opentelemetry.sdk.resources — OpenTelemetry Python documentation"
[21]: https://opentelemetry.io/docs/concepts/resources/?utm_source=chatgpt.com "Resources | OpenTelemetry"
[22]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html?utm_source=chatgpt.com "opentelemetry.sdk.trace package — OpenTelemetry Python documentation"
[23]: https://opentelemetry.io/docs/specs/otel/trace/sdk/?utm_source=chatgpt.com "Tracing SDK | OpenTelemetry"
[24]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.sampling.html?utm_source=chatgpt.com "opentelemetry.sdk.trace.sampling — OpenTelemetry Python documentation"
[25]: https://opentelemetry-python.readthedocs.io/en/latest/_modules/opentelemetry/sdk/trace/export.html?utm_source=chatgpt.com "opentelemetry.sdk.trace.export — OpenTelemetry Python documentation"
[26]: https://opentelemetry.io/docs/specs/otel/metrics/sdk/?utm_source=chatgpt.com "Metrics SDK | OpenTelemetry"
[27]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/?utm_source=chatgpt.com "Environment Variable Specification | OpenTelemetry"
[28]: https://opentelemetry-python.readthedocs.io/en/stable/sdk/_logs.html?utm_source=chatgpt.com "opentelemetry.sdk._logs package — OpenTelemetry Python documentation"
[29]: https://opentelemetry.io/docs/specs/semconv/?utm_source=chatgpt.com "OpenTelemetry semantic conventions 1.39.0 | OpenTelemetry"
[30]: https://opentelemetry.io/docs/specs/semconv/general/semantic-convention-groups/?utm_source=chatgpt.com "Semantic convention groups | OpenTelemetry"
[31]: https://opentelemetry.io/docs/specs/semconv/non-normative/http-migration/?utm_source=chatgpt.com "HTTP semantic convention stability migration | OpenTelemetry"
[32]: https://opentelemetry.io/docs/reference/specification/schemas/?utm_source=chatgpt.com "Telemetry Schemas | OpenTelemetry"

Below is the “next layer” deep dive—**OTLP exporters + distro/auto-instrumentation + resource detection + custom processors + sampling + deterministic run IDs + Hamilton run boundaries**—written for an LLM programming agent that will actually implement this in CodeAnatomy.

---

## 0) The shape of a best-in-class deployment

**Best practice shape (Python):**

1. **App/CLI emits OTLP** (traces/metrics/logs) using the Python SDK exporters.
2. **OpenTelemetry Collector** is the “control plane” for:

   * tail sampling
   * attribute/resource enrichment
   * backpressure, retry, fan-out to multiple backends
   * redaction / filtering / routing

The OpenTelemetry docs explicitly describe Collector pipelines as receivers → processors → exporters, with processor order defining the transformation order. ([OpenTelemetry][1])

**Key practical decision:**

* If you need custom processors and deep control *inside the process*, you either:

  * go **manual/programmatic SDK configuration**, or
  * ship a **custom distro/configurator** that runs at auto-instrumentation bootstrap time.
* The stock `opentelemetry-instrument` agent creates a pipeline that (per docs) you can only change via env/CLI; deeper pipeline customization means foregoing the agent and configuring in code. ([OpenTelemetry][2])

---

## 1) OTLP exporters (the egress layer you’ll actually run)

### 1.1 Protocol choice: gRPC vs HTTP/protobuf

OTLP supports:

* `grpc`
* `http/protobuf`
* optionally `http/json` (SDK dependent) ([OpenTelemetry][3])

Endpoint defaults are conventionally:

* gRPC: `4317`
* HTTP: `4318` ([OpenTelemetry][4])

**Important nuance (HTTP path construction):**

* If you set `OTEL_EXPORTER_OTLP_ENDPOINT` for **HTTP**, SDKs construct per-signal paths:

  * `/v1/traces`, `/v1/metrics`, `/v1/logs`
* If you instead set per-signal endpoints (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, etc.), that URL is used “as-is” (no path auto-appending). ([OpenTelemetry][3])

### 1.2 Canonical env vars (production “knobs”)

From the OTLP exporter spec + docs, you should treat these as “first-class”:

**Routing**

* `OTEL_EXPORTER_OTLP_ENDPOINT`
* `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
* `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
* `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` ([OpenTelemetry][4])

**Protocol**

* `OTEL_EXPORTER_OTLP_PROTOCOL`
* `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`, etc. ([OpenTelemetry][3])

**Auth headers**

* `OTEL_EXPORTER_OTLP_HEADERS` (comma-separated `k=v` list)
* and signal-specific header vars ([OpenTelemetry][3])

**Compression**

* `OTEL_EXPORTER_OTLP_COMPRESSION` (spec defines `gzip`; `none` allowed) ([OpenTelemetry][3])

**TLS / certificates / insecure**
Python’s OTLP exporter docs and env-var reference show:

* `OTEL_EXPORTER_OTLP_CERTIFICATE` / signal-specific cert vars
* `OTEL_EXPORTER_OTLP_INSECURE` / `..._TRACES_INSECURE`
* timeout vars like `OTEL_EXPORTER_OTLP_TIMEOUT` ([opentelemetry-python.readthedocs.io][5])

### 1.3 Programmatic exporters: concrete constructors & parameters (Python)

The OpenTelemetry Python OTLP exporter docs expose:

* HTTP exporters: `OTLPSpanExporter`, `OTLPMetricExporter`, `OTLPLogExporter` with params like `endpoint`, `headers`, `timeout`, `compression`, and for HTTP TLS files (`certificate_file`, `client_key_file`, `client_certificate_file`). ([opentelemetry-python.readthedocs.io][6])
* gRPC exporters: similar, with `endpoint`, `insecure`, `credentials`, `headers`, `timeout`, `compression`, `channel_options`. ([opentelemetry-python.readthedocs.io][6])

**Implementation tip:** prefer env-driven config in prod (so ops can retarget collectors/backends without code changes), but allow programmatic overrides for tests or “single-file repro harnesses”.

---

## 2) Distro + auto-instrumentation (how CodeAnatomy should bootstrap quickly)

### 2.1 What `opentelemetry-distro` actually does

The OpenTelemetry Python distro is explicitly documented as configuring:

* SDK `TracerProvider`
* `BatchSpanProcessor`
* OTLP `SpanExporter` to send data to a Collector ([OpenTelemetry][7])

Mechanically: the distro/configurator are loaded via **entry points** `opentelemetry_distro` and `opentelemetry_configurator` *before any other code executes*. ([OpenTelemetry][7])

### 2.2 `opentelemetry-instrument` + `opentelemetry-bootstrap`

* `opentelemetry-bootstrap -a install` scans installed packages and installs matching instrumentation packages. ([OpenTelemetry][8])
* `opentelemetry-instrument … python yourapp.py` wraps execution and auto-initializes instrumentors (monkey patching). ([OpenTelemetry][8])

**Critical limitation for your “custom processors” goal:**
The official auto-instrumentation example warns that the agent pipeline “cannot be modified other than through” env/CLI; for deeper customization you should configure the SDK/instrumentation in your code. ([OpenTelemetry][2])

### 2.3 Agent configuration you will actually use in CodeAnatomy

**Noise control**

* Disable specific instrumentations:

  * `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=redis,kafka,grpc_client` style (entry-point names). ([OpenTelemetry][9])
* Exclude “health” endpoints etc:

  * `OTEL_PYTHON_EXCLUDED_URLS=...`
  * `OTEL_PYTHON_<LIB>_EXCLUDED_URLS=...` ([OpenTelemetry][10])

**Log correlation & log auto-instrumentation**

* `OTEL_PYTHON_LOG_CORRELATION=true` injects trace context into logs
* `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true` enables logs auto-instrumentation (attaches an OTLP handler to root logger per docs) ([OpenTelemetry][10])

### 2.4 When you should create a CodeAnatomy custom distro/configurator

You want this if you need “zero-code” UX **and**:

* deterministic `run_id` injected everywhere
* custom SpanProcessor / LogRecordProcessor
* forced Resource detector sets
* opinionated sampling defaults per environment

Distros/configurators are a first-class extension point (entry points) and vendor distros (AWS, etc.) are configured via `OTEL_PYTHON_DISTRO` and `OTEL_PYTHON_CONFIGURATOR`. ([OpenTelemetry][7])

---

## 3) Resource detectors (making telemetry queryable in real deployments)

### 3.1 Resource ≠ span attributes

Resources are immutable “entity identity” and are associated with the provider at creation time (cannot be changed later). ([OpenTelemetry][11])
Use Resources for:

* `service.name`, `service.version`
* deployment environment
* pod/namespace/container IDs
* cloud metadata

Use **span/log attributes** for per-run/per-request things (like `run_id`) unless your process is “one run per process” (CLI job).

### 3.2 Programmatic aggregation and timeouts

Python provides `get_aggregated_resources(detectors, initial_resource=None, timeout=5)`; docs show it merges detector output and supports a per-detector timeout. ([opentelemetry-python.readthedocs.io][12])

### 3.3 Env-driven detector selection: `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`

Python exposes:

* `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` = comma-separated detector entry point names (`opentelemetry_resource_detector` group); experimental semantics. ([opentelemetry-python.readthedocs.io][5])

This is the knob you use to:

* enable container/cloud detection in services
* keep CLI startup fast (disable slow cloud metadata calls unless needed)

### 3.4 “Container-ready” distro option

`opentelemetry-container-distro` explicitly expands `opentelemetry-distro` by including additional detectors for container environments:

* docker
* kubernetes
* process ([PyPI][13])

If CodeAnatomy is frequently deployed in containers/K8s, this is an easy baseline.

---

## 4) Sampling policy (head sampling in SDK, tail sampling in Collector)

### 4.1 Head sampling (SDK): what you can do in-process

Standard env vars:

* `OTEL_TRACES_SAMPLER`
* `OTEL_TRACES_SAMPLER_ARG` ([OpenTelemetry][14])

Known sampler values include:

* `always_on`, `always_off`
* `traceidratio`
* `parentbased_traceidratio`, etc. ([OpenTelemetry][14])

**CodeAnatomy recommendation (typical):**

* Dev/local: `parentbased_traceidratio` with a higher rate (e.g., 0.2–1.0)
* Prod services: `parentbased_traceidratio` low rate (e.g., 0.01–0.05)
* Batch “investigation runs”: `always_on` (paired with Collector tail sampling / filtering)

### 4.2 Tail sampling (Collector): “keep errors, keep slow traces, sample the rest”

The OpenTelemetry blog gives a concrete `tail_sampling` processor example and emphasizes that SDKs should sample “AlwaysOn/default” so the collector sees full traces before making tail decisions. ([OpenTelemetry][15])

Example (conceptual):

```yaml
processors:
  tail_sampling:
    decision_wait: 10s
    num_traces: 50000
    expected_new_traces_per_sec: 200
    policies:
      - name: keep-errors
        type: status_code
        status_code: { status_codes: [ERROR] }
      - name: keep-slow
        type: latency
        latency: { threshold_ms: 3000 }
      - name: sample-rest
        type: probabilistic
        probabilistic: { sampling_percentage: 5 }
```

**Pipeline ordering pitfall:** don’t put `batch` *before* tail sampling or you risk splitting spans of a trace across batches (vendor collector docs explicitly warn about this). ([AWS Distro for OpenTelemetry][16])

---

## 5) Custom processors (where CodeAnatomy adds “best-in-class” value)

There are two places to enrich/transform telemetry:

### 5.1 In-process processors (SpanProcessor / LogRecordProcessor)

These are the right place for:

* ultra-cheap, deterministic enrichment (run_id lookup, pipeline ID, node name)
* last-mile correlation with in-process contextvars/baggage

**SpanProcessor requirements (spec):**

* `OnStart` and `OnEnd` are called **synchronously** and **should not block or throw**. ([OpenTelemetry][17])
  Also: span processors receive only spans where `IsRecording=true`. ([OpenTelemetry][17])

**LogRecordProcessor requirements (spec):**

* `OnEmit` is called **synchronously** and **should not block or throw**. ([OpenTelemetry][18])

**Implication:** your processors must be *O(1)* and avoid I/O. Anything heavier → Collector.

### 5.2 Collector processors (attributes/resource/filter/transform)

These are the right place for:

* PII redaction
* dropping noisy spans
* tail sampling
* routing to multiple backends
* consistent attribute normalization across services

Collector processors are explicitly intended to “modify or transform telemetry” and ordering matters. ([OpenTelemetry][1])

---

## 6) Deterministic run IDs (the CodeAnatomy “spine”)

You want a run identifier that is stable and queryable across:

* traces
* logs
* metrics
* build artifacts in DuckDB/DataFusion tables

### 6.1 Where to store run_id

* **CLI batch run (one run per process):** put `codeanatomy.run_id` on the **Resource** at startup (so every signal inherits it).
* **Long-running service handling many runs:** put run_id on:

  * a top-level “run root span”
  * and optionally propagate in context (contextvar/baggage) so child spans/logs get it.

### 6.2 Minimal in-process enrichment pattern (SpanProcessor)

Skeleton:

```python
# codeanatomy/obs/run_context.py
from contextvars import ContextVar
RUN_ID: ContextVar[str | None] = ContextVar("codeanatomy.run_id", default=None)

def set_run_id(run_id: str):
    RUN_ID.set(run_id)

def get_run_id() -> str | None:
    return RUN_ID.get()
```

```python
# codeanatomy/obs/otel_processors.py
from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.trace import Span
from .run_context import get_run_id

class RunIdSpanProcessor(SpanProcessor):
    def on_start(self, span: Span, parent_context=None) -> None:
        rid = get_run_id()
        if rid:
            span.set_attribute("codeanatomy.run_id", rid)

    def on_end(self, span) -> None:
        # keep empty or add cheap “end-only” tags; must not block
        pass

    def shutdown(self): pass
    def force_flush(self, timeout_millis: int = 30000): return True
```

This is spec-compliant as long as it is non-blocking and constant-time. ([OpenTelemetry][17])

### 6.3 Logs enrichment options

* If you use **Python logging integration** with OTel, you can export logs via SDK wiring. ([OpenTelemetry][19])
* If you use **agent mode**, enable:

  * `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true`
  * `OTEL_PYTHON_LOG_CORRELATION=true` ([OpenTelemetry][9])
* If you need guaranteed `run_id` on every log record, add a `LogRecordProcessor` that copies the contextvar into log attributes (must be non-blocking). ([OpenTelemetry][18])

---

## 7) Hamilton run boundaries (turn the DAG into trace structure)

Your goal: every Hamilton run becomes a **trace subtree** that mirrors DAG structure:

### 7.1 Spans you want

* `codeanatomy.hamilton.run` (root span for a run)

  * attrs: `run_id`, `repo_fingerprint`, `plan_fingerprint`, `outputs`, `module_set`, etc.
* `codeanatomy.hamilton.node` (one per node execution)

  * attrs: `node_name`, `tags`, `cache_hit`, `input_shapes`, `output_schema_hash`, etc.

### 7.2 Why this is the right approach

* You can later do Collector tail sampling based on:

  * error status
  * latency
  * custom attributes (`run_id`, `node_name`, `cache_hit=false`, etc.) ([OpenTelemetry][15])

### 7.3 Minimal wrapper pattern (manual instrumentation)

```python
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from .run_context import set_run_id

tracer = trace.get_tracer("codeanatomy.hamilton")

def run_hamilton(driver, *, run_id: str, **kwargs):
    set_run_id(run_id)
    with tracer.start_as_current_span(
        "codeanatomy.hamilton.run",
        attributes={"codeanatomy.run_id": run_id},
    ) as run_span:
        try:
            return driver.execute(**kwargs)
        except Exception as e:
            run_span.record_exception(e)
            run_span.set_status(Status(StatusCode.ERROR))
            raise
```

Then inside Hamilton execution hooks (or in wrappers around node callables), create per-node spans.

---

## 8) Concrete Collector configs (dev + prod)

### 8.1 “Local dev: show everything”

OpenTelemetry Python exporter docs provide a minimal Collector config that:

* receives OTLP over gRPC 4317 and HTTP 4318
* uses `debug` exporter (console) for traces/metrics/logs ([OpenTelemetry][20])

### 8.2 “Prod: tail sampling + batching + backpressure”

Collector config fundamentals (receivers/processors/exporters/service pipelines) are documented; processors must be enabled in pipelines and ordering matters. ([OpenTelemetry][1])

---

## 9) Recommended integration strategy for CodeAnatomy (no ambiguity)

### Mode A (services): **auto-instrumentation + custom distro**

Use when you want “zero-code enablement” for FastAPI/FastMCP/gRPC services:

* `opentelemetry-distro[otlp]`, `opentelemetry-instrumentation`, `opentelemetry-bootstrap`
* env var config for exporters/sampling/resource detectors
* custom distro/configurator if you must inject RunIdSpanProcessor automatically ([OpenTelemetry][7])

### Mode B (CLI/batch): **manual/programmatic SDK config**

Use when a single CLI invocation corresponds to one run and you want deterministic run IDs on Resource and full control over processors—because the agent pipeline is intentionally constrained. ([OpenTelemetry][2])

---

[1]: https://opentelemetry.io/docs/collector/configuration/?utm_source=chatgpt.com "Configuration | OpenTelemetry"
[2]: https://opentelemetry.io/docs/zero-code/python/example/?utm_source=chatgpt.com "Auto-Instrumentation Example | OpenTelemetry"
[3]: https://opentelemetry.io/docs/specs/otel/protocol/exporter/?utm_source=chatgpt.com "OpenTelemetry Protocol Exporter | OpenTelemetry"
[4]: https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/?utm_source=chatgpt.com "OTLP Exporter Configuration | OpenTelemetry"
[5]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/environment_variables.html?utm_source=chatgpt.com "opentelemetry.sdk.environment_variables — OpenTelemetry Python documentation"
[6]: https://opentelemetry-python.readthedocs.io/en/latest/exporter/otlp/otlp.html "OpenTelemetry OTLP Exporters — OpenTelemetry Python  documentation"
[7]: https://opentelemetry.io/docs/languages/python/distro/?utm_source=chatgpt.com "OpenTelemetry Distro | OpenTelemetry"
[8]: https://opentelemetry.io/docs/zero-code/python/?utm_source=chatgpt.com "Python zero-code instrumentation | OpenTelemetry"
[9]: https://opentelemetry.io/ro/docs/zero-code/python/configuration/?utm_source=chatgpt.com "Agent Configuration | OpenTelemetry"
[10]: https://opentelemetry.io/fr/docs/zero-code/python/configuration/?utm_source=chatgpt.com "Configuration de l'agent | OpenTelemetry"
[11]: https://opentelemetry.io/docs/specs/otel/resource/sdk/?utm_source=chatgpt.com "Resource SDK | OpenTelemetry"
[12]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/resources.html?utm_source=chatgpt.com "opentelemetry.sdk.resources package — OpenTelemetry Python documentation"
[13]: https://pypi.org/project/opentelemetry-container-distro/?utm_source=chatgpt.com "opentelemetry-container-distro · PyPI"
[14]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/?utm_source=chatgpt.com "Environment Variable Specification | OpenTelemetry"
[15]: https://opentelemetry.io/blog/2022/tail-sampling/?utm_source=chatgpt.com "Tail Sampling with OpenTelemetry: Why it’s useful, how to do it, and what to consider | OpenTelemetry"
[16]: https://aws-otel.github.io/docs/components/processors?utm_source=chatgpt.com "Processors | AWS Distro for OpenTelemetry"
[17]: https://opentelemetry.io/docs/specs/otel/trace/sdk/ "Tracing SDK | OpenTelemetry"
[18]: https://opentelemetry.io/docs/specs/otel/logs/sdk/ "Logs SDK | OpenTelemetry"
[19]: https://opentelemetry.io/docs/languages/python/instrumentation/?utm_source=chatgpt.com "Instrumentation | OpenTelemetry"
[20]: https://opentelemetry.io/docs/languages/python/exporters/?utm_source=chatgpt.com "Exporters | OpenTelemetry"

Below is an “LLM-agent grade” deep dive on the **instrumentation packages** you listed, organized the same way as the earlier SDK/exporter deep dives: **what the instrumentation wraps**, **what spans/metrics it emits**, **how to tune/suppress it**, and **where the sharp edges are**.

---

## 0) Cross-cutting model of Python “contrib” instrumentations

### 0.1 API contract: instrumentations patch libraries; the app provides the SDK

OpenTelemetry Python instrumentations are typically **`BaseInstrumentor`** implementations that:

* declare which third-party packages they apply to (`instrumentation_dependencies()` pattern) so auto-instrumentation can safely skip them when the target library isn’t installed ([OpenTelemetry Python Contrib][1])
* “instrument” by **monkey-patching** functions (via `wrapt`) or by inserting middleware/interceptors (ASGI/gRPC)
* rely on the application’s configured **SDK providers/exporters/processors** to actually record and export telemetry (instrumentations should not force SDK choices) ([GitHub][2])

### 0.2 Configuration surfaces you’ll keep using

**Excluded URLs (regex lists)** are a very common control surface:

* Global: `OTEL_PYTHON_EXCLUDED_URLS`
* Per-instrumentation: `OTEL_PYTHON_<LIB>_EXCLUDED_URLS` (exact env keys vary by instrumentation; the contrib docs for each are authoritative) ([OpenTelemetry][3])

**Hooks** are the other common surface:

* “request_hook” / “response_hook” style callbacks that run *after span creation* and *before span end* (must be fast and non-blocking).

**Semantic convention stability / dual-emission**
Many HTTP instrumentations now internally support a “stability mode” where they may emit “old” vs “new” semantic convention attributes/metrics based on an opt-in mode initialized at instrumentation time. You’ll see this as `_OpenTelemetrySemanticConventionStability._initialize()` + `_get_opentelemetry_stability_opt_in_mode(_OpenTelemetryStabilitySignalType.HTTP, ...)` inside the instrumentation. ([OpenTelemetry Python Contrib][4])

### 0.3 Avoiding double-instrumentation

Two important patterns appear in the HTTP client stack:

* **Layered libraries** (e.g., `requests` uses `urllib3` under the hood). Both have instrumentations.
* The contrib instrumentations include **suppression mechanisms** to avoid nested spans when one instrumented layer calls another instrumented layer (e.g., `requests` wraps its actual send inside `suppress_http_instrumentation()`; `urllib3` does similarly around the wrapped call). ([OpenTelemetry Python Contrib][5])

Even with suppression, you should still pick an intentional “ownership” layer for spans/metrics (e.g., trace at `requests` level OR `urllib3` level, not both) unless you explicitly want both perspectives.

---

## 1) Server / ASGI (FastAPI / Starlette / “anything ASGI”, including FastMCP)

### 1.1 `opentelemetry-instrumentation-asgi` (the core primitive)

#### What it wraps

`OpenTelemetryMiddleware(app, ...)` is a generic ASGI middleware that:

* starts a **server span** for each incoming HTTP request (and websockets)
* can optionally create additional internal spans around ASGI `receive` / `send` events (noise vs insight tradeoff)
* can be configured with `excluded_urls`, hooks, tracer/meter providers, and **`exclude_spans=['receive','send']`** to turn off those internal spans ([OpenTelemetry Python Contrib][6])

#### Span naming and “route-ish” attributes

The default span naming logic is defined by `get_default_span_details(scope)`:

* for HTTP: `"METHOD path"` when both exist; otherwise just method or path depending on scope type ([OpenTelemetry Python Contrib][7])
  For metrics, it also tries to collect a **parameterized target** suitable for HTTP metrics (e.g., `/api/users/{user_id}`) by inspecting framework-specific route metadata (FastAPI route `path_format` when present). ([OpenTelemetry Python Contrib][7])

#### Hooks (3 kinds)

* `server_request_hook(span, scope)` — invoked for each request with the server span and ASGI scope
* `client_request_hook(span, scope, message)` — invoked when `receive()` is called
* `client_response_hook(span, scope, message)` — invoked when `send()` is called ([OpenTelemetry Python Contrib][6])

These are the “right place” to attach your run_id / build-step identifiers (cheap attributes only).

#### Header capture (server-side)

ASGI middleware supports capturing selected request/response headers as span attributes via env vars:

* `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`
* `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`
* sanitization: `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS` (regex allowed; case-insensitive) ([OpenTelemetry Python Contrib][8])

> Note: the ASGI source explicitly calls out these header-capture env var names as “experimental / subject to change”. ([OpenTelemetry Python Contrib][8])

---

### 1.2 `opentelemetry-instrumentation-starlette`

Starlette instrumentation is effectively a **Starlette-aware wrapper around ASGI middleware**:

#### What it wraps

It instruments a Starlette app by adding `OpenTelemetryMiddleware` with Starlette-specific `default_span_details` so it can compute route-ish naming and `http.route`. ([OpenTelemetry Python Contrib][9])

#### Excluded URL controls

* `OTEL_PYTHON_STARLETTE_EXCLUDED_URLS` (and fallback `OTEL_PYTHON_EXCLUDED_URLS`) ([OpenTelemetry Python Contrib][10])

#### Hooks

Same conceptual hook trio as ASGI (server request + receive + send), exposed on the Starlette instrumentor as request/response hooks. ([OpenTelemetry Python Contrib][10])

#### Span naming / `http.route`

Starlette instrumentation inspects the Starlette route matching against the ASGI scope to find a route template and then sets:

* span name: `"{method} {route}"` (or route alone for websocket)
* attribute: `http.route = route` when available ([OpenTelemetry Python Contrib][9])

This is usually what you want for high-cardinality control: prefer template routes over raw paths.

---

### 1.3 `opentelemetry-instrumentation-fastapi`

FastAPI instrumentation is again a FastAPI-aware wrapper around ASGI middleware, but with **extra work to capture exceptions correctly**.

#### What it wraps / why it’s special

The FastAPI instrumentor constructs a Starlette middleware stack and then wraps it so that:

* an active tracing context exists for user-defined handlers
* exceptions are recorded onto the active span (because `OpenTelemetryMiddleware` can end spans before Starlette’s exception handling runs in some flows)
  It does this by re-wrapping parts of Starlette’s `ServerErrorMiddleware` stack and inserting a custom `ExceptionHandlerMiddleware` that records exceptions to the current span and sets error status. ([OpenTelemetry Python Contrib][4])

#### Excluded URL controls

* `OTEL_PYTHON_FASTAPI_EXCLUDED_URLS` (fallback `OTEL_PYTHON_EXCLUDED_URLS`) ([OpenTelemetry Python Contrib][4])

#### Hooks and noise control

`FastAPIInstrumentor.instrument_app(..., exclude_spans=['receive','send'], ...)` supports:

* the same hook trio (server request + receive + send)
* `exclude_spans` to suppress internal `receive`/`send` spans ([OpenTelemetry Python Contrib][4])

#### Header capture

FastAPI instrumentation supports the same server header capture env vars and also allows passing header capture lists directly to `instrument_app()`; sanitization is supported similarly. ([OpenTelemetry Python Contrib][4])

**Practical rule:** If you’re on FastAPI, use the FastAPI instrumentation rather than Starlette/ASGI directly unless you have a very specific reason—FastAPI’s instrumentation is explicitly engineered around FastAPI/Starlette error handling semantics. ([OpenTelemetry Python Contrib][4])

---

## 2) gRPC (`opentelemetry-instrumentation-grpc`)

### 2.1 What it instruments: interceptors for sync + asyncio gRPC

The gRPC contrib instrumentation provides:

* global server/client instrumentors (`GrpcInstrumentorServer`, `GrpcInstrumentorClient`)
* asyncio variants (`GrpcAioInstrumentorServer`, `GrpcAioInstrumentorClient`)
* and *manual interceptor helpers* so you can attach interceptors to channels/servers yourself. ([OpenTelemetry Python Contrib][1])

### 2.2 Filtering: reduce noise and avoid health-check spam

Two layers of filtering are supported:

1. **Programmatic filters** attached to instrumentors/interceptors:

* `filters.any_of(...)`, `filters.method_name(...)`, `filters.method_prefix(...)`
* `filters.negate(...)`
  These can be applied globally (instrumentor) or directly on a manually created interceptor. ([OpenTelemetry Python Contrib][1])

2. **Env var excluded services**

* `OTEL_PYTHON_GRPC_EXCLUDED_SERVICES="GRPCTestServer,GRPCHealthServer"` adds filters to exclude whole service names automatically. ([OpenTelemetry Python Contrib][1])

### 2.3 Hooks: request/response hooks exist at interceptor construction

The public API to build interceptors includes hook parameters such as `request_hook` / `response_hook` for client interceptor construction. ([OpenTelemetry Python Contrib][1])

### 2.4 Semantic conventions + sensitive metadata

The gRPC semantic conventions explicitly warn that capturing request/response metadata values can be a **security risk** and SHOULD require explicit configuration. ([OpenTelemetry][11])
For “best-in-class”, treat metadata capture as *opt-in only*, and prefer narrow allowlists.

---

## 3) Outbound HTTP client instrumentations

All of these share the same conceptual contract:

* **SpanKind.CLIENT** spans around a request
* use **`propagate.inject(...)`** to add trace context headers into outgoing requests (where headers are mutable)
* have hooks (request/response) for enrichment
* use exclude-url regex lists to skip endpoints
* often emit duration histograms (metrics) when a MeterProvider is configured

### 3.1 `opentelemetry-instrumentation-requests`

#### What it patches

It wraps `requests.sessions.Session.send` (covers `requests.get/post/etc.`). ([OpenTelemetry Python Contrib][5])

#### Hooks

* `request_hook(span, prepared_request)`
* `response_hook(span, prepared_request, response)` ([OpenTelemetry Python Contrib][5])

#### Excluded URLs

* `OTEL_PYTHON_REQUESTS_EXCLUDED_URLS` (fallback `OTEL_PYTHON_EXCLUDED_URLS`) ([OpenTelemetry Python Contrib][5])

#### Metrics knobs

Requests instrumentation emits HTTP client duration metrics, and supports customizing duration histogram boundaries via `duration_histogram_boundaries=[...]`. ([OpenTelemetry Python Contrib][5])

#### Suppression (important if urllib3 is also instrumented)

It injects headers and wraps the actual send inside `suppress_http_instrumentation()` to prevent nested instrumentation further down the stack. ([OpenTelemetry Python Contrib][5])

---

### 3.2 `opentelemetry-instrumentation-httpx`

#### What it patches

It instruments HTTPX at the **transport layer**:

* wraps `httpx.HTTPTransport.handle_request`
* wraps `httpx.AsyncHTTPTransport.handle_async_request`
  It also supports per-client instrumentation (`instrument_client`) and uninstrumenting specific clients. ([OpenTelemetry Python Contrib][12])

#### Hooks (sync + async)

* sync `request_hook(span, request_tuple)` / `response_hook(span, request_tuple, response_tuple)`
* async equivalents (`async_request_hook`, `async_response_hook`)
  The docs emphasize that hooks see **raw transport arguments/return values** (method/url/headers/stream/extensions tuples), so your hook must know that shape. ([OpenTelemetry Python Contrib][12])

#### Excluded URLs

* `OTEL_PYTHON_HTTPX_EXCLUDED_URLS` (fallback `OTEL_PYTHON_EXCLUDED_URLS`) ([OpenTelemetry Python Contrib][12])

#### Metrics + semconv stability

The HTTPX implementation creates “old” vs “new” duration histograms depending on semantic convention opt-in mode, and records `error.type` on exceptions in “new” mode. ([OpenTelemetry Python Contrib][13])

---

### 3.3 `opentelemetry-instrumentation-urllib3`

#### What it patches

It wraps `urllib3.connectionpool.HTTPConnectionPool.urlopen`, creating a CLIENT span per request. ([OpenTelemetry Python Contrib][14])

#### URL filtering (PII control)

Unlike `requests`, urllib3 instrumentation explicitly supports `url_filter(url: str) -> str` for sanitization (classic use: strip query params). ([OpenTelemetry Python Contrib][15])

#### Hooks

* `request_hook(span, pool, request_info)`
* `response_hook(span, pool, response)` ([OpenTelemetry Python Contrib][15])

#### Excluded URLs

* `OTEL_PYTHON_URLLIB3_EXCLUDED_URLS` (fallback `OTEL_PYTHON_EXCLUDED_URLS`) ([OpenTelemetry Python Contrib][15])

#### Metrics beyond duration

The urllib3 instrumentation shows creation of:

* duration histogram(s)
* request/response body size histograms (old/new semconv paths) ([OpenTelemetry Python Contrib][14])

#### Header injection + suppression

It calls `inject(headers)` before the wrapped request and uses suppression around the underlying call path. ([OpenTelemetry Python Contrib][14])

---

### 3.4 `opentelemetry-instrumentation-urllib`

#### What it patches and why you’d use it

This targets Python’s stdlib `urllib` request machinery (useful when dependencies call urllib directly).

#### Hooks

* `request_hook(span, urllib.request.Request)`
* `response_hook(span, request, http.client.HTTPResponse)` ([OpenTelemetry Python Contrib][16])

#### Excluded URLs

* `OTEL_PYTHON_URLLIB_EXCLUDED_URLS` (fallback `OTEL_PYTHON_EXCLUDED_URLS`) ([OpenTelemetry Python Contrib][16])

Also supports uninstrumenting a specific `OpenerDirector` (useful for libraries that create custom openers). ([OpenTelemetry Python Contrib][16])

---

### 3.5 `opentelemetry-instrumentation-aiohttp-client` (if/when you add aiohttp)

#### How it integrates

The contrib implementation instruments `aiohttp.ClientSession` and supports:

* `url_filter` for sanitizing requested URLs
* request/response hooks based on aiohttp tracing events
* optional `trace_configs` so you can plug into aiohttp’s native tracing lifecycle for richer event-level annotations ([OpenTelemetry Python Contrib][17])

#### Excluded URLs

* `OTEL_PYTHON_AIOHTTP_CLIENT_EXCLUDED_URLS` (and it also references `OTEL_PYTHON_EXCLUDED_URLS` as a broader fallback) ([OpenTelemetry Python Contrib][17])

---

## 4) Logging + correlation (`opentelemetry-instrumentation-logging`)

### 4.1 What it actually does (important: it’s not “log exporting” by itself)

The logging instrumentation:

* registers a **custom log record factory** that injects trace context into each LogRecord
* injects these keys: `otelSpanID`, `otelTraceID`, `otelServiceName`, `otelTraceSampled`
* optionally calls `logging.basicConfig()` with a default format that prints those values ([OpenTelemetry Python Contrib][18])

### 4.2 Enablement / ordering pitfalls

It is **opt-in**:

* set `OTEL_PYTHON_LOG_CORRELATION=true` to enable and have it call `basicConfig()` automatically ([OpenTelemetry Python Contrib][18])
  If you set your logging format manually (or your framework does), you must enable the instrumentation **before** the format is applied; otherwise log statements can hit KeyError for missing injected fields (logging swallows but you lose messages). ([OpenTelemetry Python Contrib][18])

### 4.3 Why this matters for build pipelines

For long, multi-step pipelines (like your batch builds), the value is that a single trace ID can “join” logs ↔ spans even when spans are sampled down (you still get correlation on sampled traces; `otelTraceSampled` tells you whether a trace is sampled). ([OpenTelemetry Python Contrib][18])

---

## 5) Async & concurrency instrumentations

### 5.1 `opentelemetry-instrumentation-asyncio`

#### What it targets

It instruments asyncio workflows and can trace:

* specific coroutine names
* futures (when enabled)
* functions executed via `asyncio.to_thread`
  It also emits metrics for coroutine/future duration and counts even if spans are not generated. ([OpenTelemetry Python Contrib][19])

#### Key env vars

* `OTEL_PYTHON_ASYNCIO_COROUTINE_NAMES_TO_TRACE=...`
* `OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED=true`
* `OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE=...` ([OpenTelemetry Python Contrib][19])

#### Metric names (from docs)

* `asyncio.process.duration` (seconds)
* `asyncio.process.count` (count) ([OpenTelemetry Python Contrib][19])

### 5.2 `opentelemetry-instrumentation-threading`

This one is subtle but crucial for mixed concurrency:

#### What it does (and does not do)

* It **does not generate telemetry**.
* It ensures OpenTelemetry **Context propagation across threads**, so spans created in worker threads are properly parented. ([OpenTelemetry Python Contrib][20])

#### Coverage

When instrumented, it propagates context into:

* `threading.Thread`
* `threading.Timer`
* and threads created by `futures.ThreadPoolExecutor` (worker threads) ([OpenTelemetry Python Contrib][20])

---

## 6) Host/process metrics (`opentelemetry-instrumentation-system-metrics`)

### 6.1 What it emits

This is a metrics-only instrumentation that collects **system + process metrics**, and it’s designed to run with a configured MeterProvider + MetricReader (e.g., periodic exporting). ([OpenTelemetry Python Contrib][21])

### 6.2 Configuration shape

Docs show a `config` dict mapping metric names to selected “sub-measures”, e.g.:

* `system.memory.usage`: `["used","free","cached"]`
* `system.cpu.time`: `["idle","user","system","irq"]`
* process metrics (`process.memory.usage`, `process.cpu.time`, etc.) ([OpenTelemetry Python Contrib][21])

It also notes deprecations (out-of-spec `process.runtime.*` metrics) and encourages moving to process-prefixed metrics. ([OpenTelemetry Python Contrib][21])

### 6.3 Operational note

It collects metrics asynchronously; your exporter/reader interval controls cost and granularity. ([OpenTelemetry Python Contrib][21])

---

## 7) DB / storage baseline (`opentelemetry-instrumentation-dbapi`)

### 7.1 What it is for

DB-API instrumentation is a **generic base** for libraries following Python DB-API v2.0 (PEP 249). It’s often used internally by driver/ORM-specific instrumentations; you typically only use it directly when a dedicated instrumentation doesn’t exist. ([OpenTelemetry Python Contrib][22])

### 7.2 How it works

The API centers on `trace_integration(connect_module, connect_method_name, database_system, ...)` which wraps the connect method and returns instrumented connection/cursor proxies. ([OpenTelemetry Python Contrib][22])

It supports:

* `connection_attributes` mapping (how to extract host/port/database/user from connection)
* `capture_parameters` (capture `db.statement.parameters`—high risk for sensitive data)
* `enable_commenter` (sqlcommenter)
* `enable_attribute_commenter` (include sqlcomment in `db.statement` span attribute) ([OpenTelemetry Python Contrib][22])

The implementation wraps cursor methods like `execute`, `executemany`, `callproc` through traced proxies. ([OpenTelemetry Python Contrib][23])

### 7.3 sqlcommenter duplication pitfall

If you enable sqlcommenter at multiple layers (framework + driver), you can end up with duplicated comments appended to queries—OpenTelemetry’s Django instrumentation docs explicitly warn about this pattern. ([OpenTelemetry Python Contrib][24])

---

## High-signal takeaways for an implementation agent (without repo-specific code)

1. Prefer **FastAPI/Starlette instrumentations** over raw ASGI when you want correct route naming and framework-aware behavior—FastAPI’s instrumentation explicitly compensates for exception lifecycle issues. ([OpenTelemetry Python Contrib][4])
2. For HTTP clients: choose one “span ownership layer” (requests vs urllib3). Suppression exists, but clarity wins. ([OpenTelemetry Python Contrib][5])
3. Use hooks for **cheap, deterministic enrichment** (run_id, node_name, cache_hit), and use exclude lists/filters to keep cardinality and cost under control. ([OpenTelemetry Python Contrib][6])
4. Treat anything that captures headers/parameters/metadata as **explicit allowlist + redaction** territory (gRPC metadata guidance is explicit; DB capture parameters is risky). ([OpenTelemetry][11])

[1]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/grpc/grpc.html?utm_source=chatgpt.com "OpenTelemetry gRPC Instrumentation — OpenTelemetry Python Contrib documentation"
[2]: https://github.com/open-telemetry/opentelemetry-python-contrib?utm_source=chatgpt.com "GitHub - open-telemetry/opentelemetry-python-contrib: OpenTelemetry instrumentation for Python modules"
[3]: https://opentelemetry.io/ro/docs/zero-code/python/configuration/?utm_source=chatgpt.com "Agent Configuration | OpenTelemetry"
[4]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/fastapi.html "opentelemetry.instrumentation.fastapi — OpenTelemetry Python Contrib  documentation"
[5]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/requests.html "opentelemetry.instrumentation.requests — OpenTelemetry Python Contrib  documentation"
[6]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/asgi/asgi.html?utm_source=chatgpt.com "OpenTelemetry ASGI Instrumentation — OpenTelemetry Python Contrib documentation"
[7]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/asgi.html?utm_source=chatgpt.com "opentelemetry.instrumentation.asgi — OpenTelemetry Python Contrib documentation"
[8]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/asgi.html "opentelemetry.instrumentation.asgi — OpenTelemetry Python Contrib  documentation"
[9]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/starlette.html?utm_source=chatgpt.com "opentelemetry.instrumentation.starlette — OpenTelemetry Python Contrib documentation"
[10]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/starlette/starlette.html?utm_source=chatgpt.com "OpenTelemetry Starlette Instrumentation — OpenTelemetry Python Contrib documentation"
[11]: https://opentelemetry.io/docs/specs/semconv/rpc/grpc/?utm_source=chatgpt.com "Semantic conventions for gRPC | OpenTelemetry"
[12]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/httpx.html "opentelemetry.instrumentation.httpx — OpenTelemetry Python Contrib  documentation"
[13]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/httpx.html?utm_source=chatgpt.com "opentelemetry.instrumentation.httpx — OpenTelemetry Python Contrib documentation"
[14]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/urllib3.html "opentelemetry.instrumentation.urllib3 — OpenTelemetry Python Contrib  documentation"
[15]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/urllib3/urllib3.html?utm_source=chatgpt.com "OpenTelemetry urllib3 Instrumentation — OpenTelemetry Python Contrib documentation"
[16]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/urllib/urllib3.html?utm_source=chatgpt.com "OpenTelemetry urllib Instrumentation — OpenTelemetry Python Contrib documentation"
[17]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/aiohttp_client.html?utm_source=chatgpt.com "opentelemetry.instrumentation.aiohttp_client — OpenTelemetry Python Contrib documentation"
[18]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html?utm_source=chatgpt.com "OpenTelemetry Logging Instrumentation — OpenTelemetry Python Contrib documentation"
[19]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/asyncio/asyncio.html?utm_source=chatgpt.com "OpenTelemetry asyncio Instrumentation — OpenTelemetry Python Contrib documentation"
[20]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/threading/threading.html?utm_source=chatgpt.com "OpenTelemetry Threading Instrumentation — OpenTelemetry Python Contrib documentation"
[21]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/system_metrics/system_metrics.html?utm_source=chatgpt.com "OpenTelemetry system metrics Instrumentation — OpenTelemetry Python Contrib documentation"
[22]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/dbapi/dbapi.html?utm_source=chatgpt.com "OpenTelemetry Database API Instrumentation — OpenTelemetry Python Contrib documentation"
[23]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/dbapi.html?utm_source=chatgpt.com "opentelemetry.instrumentation.dbapi — OpenTelemetry Python Contrib documentation"
[24]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/django/django.html?utm_source=chatgpt.com "OpenTelemetry Django Instrumentation — OpenTelemetry Python Contrib documentation"

Below is a **gap analysis** of the *capabilities that exist* in the exact libraries/categories we’ve discussed, but that I **have not yet covered in comprehensive technical depth** (i.e., enough detail for an LLM programming agent to fully exploit/tune them in a real deployment).

---

## A) Python agent + env/CLI configuration plane gaps

### A1) Python-agent knobs beyond “excluded URLs”

I referenced excluded URLs and disabling instrumentations, but I have **not** deeply covered these additional agent controls and their operational implications:

* **Request attribute extraction** env vars (framework-specific): e.g., `OTEL_PYTHON_*_TRACED_REQUEST_ATTRS` for Django/Falcon/Tornado to extract named request fields into span attrs (cardinality/PII impact; ordering; failure modes). ([OpenTelemetry][1])
* **Logging plane** env vars: `OTEL_PYTHON_LOG_FORMAT`, `OTEL_PYTHON_LOG_LEVEL`, plus the semantic difference between `OTEL_PYTHON_LOG_CORRELATION` and **logs auto-instrumentation** (`OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED`, attaches an OTLP handler to the root logger). ([OpenTelemetry][1])
* **Gevent patching** before SDK init: `OTEL_PYTHON_AUTO_INSTRUMENTATION_EXPERIMENTAL_GEVENT_PATCH=patch_all` (important if you ever use gevent/monkeypatch). ([OpenTelemetry][1])
* **ID generator selection** via `OTEL_PYTHON_ID_GENERATOR` (and how that affects interoperability with backends that expect X-Ray-style IDs, etc.). ([OpenTelemetry][1])

### A2) Exporter selection semantics

I did not deeply cover the *full* standardized exporter-selection behavior of:

* `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` including `none`, and “in-development” values such as `otlp/stdout` (relevant for dev/test harnesses). ([OpenTelemetry][2])

### A3) Reloader / debug-mode pitfalls

I mentioned “auto-instrumentation can be constrained,” but I didn’t cover **real-world failure modes** like Flask debug reloader breaking instrumentation and the recommended mitigation (`use_reloader=False`). This pattern generalizes to other reloaders. ([OpenTelemetry][3])

---

## B) Semantic conventions stability + schema URL (major missing deep dive)

### B1) The *migration mechanism* you must account for: `OTEL_SEMCONV_STABILITY_OPT_IN`

I mentioned semconv stability, but did not fully detail the **category-specific opt-in modes** and how they change emitted attributes/metric names/units:

* HTTP: `http` vs `http/dup` migration behavior and why you must update dashboards/alerts accordingly. ([OpenTelemetry][4])
* Database: `database` vs `database/dup`. ([OpenTelemetry][5])
* RPC: `rpc` vs `rpc/dup`. ([OpenTelemetry][6])

### B2) Instrumentations actively switch behavior based on stability mode (and set schema URLs)

Several contrib instrumentations (HTTP especially) internally initialize stability mode and emit **old vs new** metrics/attrs accordingly; I have not yet mapped those branches systematically (what changes, when, and what schema URL gets set).

Example (client): urllib3 instrumentation creates **old** metrics (`http.client.duration`, `http.client.request.size`, `http.client.response.size`) *and/or* **new** metrics (`http.client.request.duration`, `http.client.request.body.size`, `http.client.response.body.size`) depending on stability mode. ([opentelemetry-python-contrib.readthedocs.io][7])
Example (server frameworks): FastAPI instrumentation explicitly initializes stability mode during `instrument_app`. ([opentelemetry-python-contrib.readthedocs.io][8])

---

## C) HTTP server instrumentations (ASGI/FastAPI/Starlette): gaps

### C1) Full emitted span/metric surface vs the spec

I described hooks/exclusions/header capture, but I didn’t provide a **complete mapping** of:

* Required vs recommended attributes for HTTP spans/metrics
* Cardinality-sensitive attributes (e.g., route templates vs raw paths)
* Method canonicalization rules (`_OTHER`, `http.request.method_original`)
* Opt-in attributes and their security warnings (e.g., server.address/port) ([OpenTelemetry][9])

### C2) WebSocket behavior and span topology

ASGI middleware has explicit branching for websocket span naming and sets status codes for websocket send, and can generate internal send/receive spans (which you can exclude). I haven’t provided a comprehensive “what spans exist, when, and how to tune them” model for websocket-heavy apps. ([opentelemetry-python-contrib.readthedocs.io][10])

### C3) Header capture: regex semantics + experimental stability

I covered header capture conceptually, but didn’t fully detail:

* case-insensitive matching and regex matching
* sanitization mechanics and the “[REDACTED]” behavior
* the explicit “still experimental and may change” warning ([opentelemetry-python-contrib.readthedocs.io][11])

### C4) Server metrics beyond duration

I did not cover whether/when the Python HTTP server instrumentations emit optional metrics like:

* `http.server.request.body.size`
* `http.server.response.body.size`
* `http.server.active_requests`
  …which are defined in the HTTP metrics semconv as optional/required (and are often behind “experimental telemetry” toggles in other ecosystems). ([OpenTelemetry][9])

---

## D) HTTP client instrumentations (requests/httpx/urllib3/urllib/aiohttp-client): gaps

### D1) Per-client vs global instrumentation controls

I noted global patching; I didn’t deeply cover **per-instance controls** and their relevance to embedded SDKs:

* HTTPX supports instrumenting **only specific client instances** and uninstrumenting specific clients. ([opentelemetry-python-contrib.readthedocs.io][12])
* Aiohttp-client supports per-session instrumentation via `create_trace_config(...)` and optional `trace_configs` for deeper lifecycle enrichment. ([opentelemetry-python-contrib.readthedocs.io][13])

### D2) Metrics: names, units, bucket boundaries, and configurability

I haven’t fully covered:

* The **spec-required** bucket boundaries for `http.client.request.duration` (and how that differs from legacy histograms). ([OpenTelemetry][9])
* Requests’ explicit support for custom histogram boundaries at instrumentation time. ([opentelemetry-python-contrib.readthedocs.io][14])
* urllib3’s request/response size histograms in both old/new semconv modes (and the fact that size metrics move from `*.size` to `*.body.size`). ([opentelemetry-python-contrib.readthedocs.io][7])

### D3) URL sanitization/redaction strategy

I mentioned “url_filter / redaction” but didn’t cover it as a first-class deployment design:

* aiohttp-client explicitly supports a `url_filter` for removing query params/PII. ([opentelemetry-python-contrib.readthedocs.io][13])
* Many HTTP client instrumentations have internal redaction/sanitization steps; you need a coherent policy across libraries, especially when dependencies stack (requests → urllib3). ([opentelemetry-python-contrib.readthedocs.io][15])

### D4) Suppression / double-instrumentation

I pointed out suppression exists, but didn’t deeply cover:

* the suppression mechanism’s guarantees and failure modes
* how to intentionally pick “ownership layer” (requests vs urllib3) while still keeping useful metrics ([opentelemetry-python-contrib.readthedocs.io][15])

### D5) Client header capture (status: uneven / distro-dependent)

Upstream OpenTelemetry Python docs prominently document **server** header capture env vars; I did not cover **client header capture** controls because it is not consistently documented upstream the same way. Some distributions (e.g., Elastic’s EDOT Python agent) document `OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_CLIENT_REQUEST/RESPONSE` for client instrumentations. This is a meaningful capability to consider, but needs “which distro are we using?” clarity. ([OpenTelemetry][3])

---

## E) gRPC instrumentation: gaps

### E1) Full RPC/gRPC semantic conventions mapping

I haven’t provided a full “what attrs must/should be set” deep dive for:

* span name rules (`{rpc.method}`), fallback behavior, and `_OTHER` + `rpc.method_original` requirements
* `rpc.response.status_code` and `error.type` rules (including “low-cardinality error identifiers” guidance)
* streaming lifecycle coverage (“span covers full lifetime of request/response streams”) ([OpenTelemetry][6])

### E2) RPC metrics (and whether Python grpc instrumentation emits them)

RPC semconv defines recommended histograms for `rpc.client.call.duration` / `rpc.server.call.duration` with explicit bucket boundaries; I did not cover:

* whether `opentelemetry-instrumentation-grpc` in Python emits these metrics today
* if it does, which attribute sets it uses (method/service/status/system) ([OpenTelemetry][16])

### E3) Filtering beyond “exclude services”

I referenced filters and excluded services, but didn’t deeply cover:

* composing filters (any_of / negate / prefix / name) as a noise-control language
* how to apply filters consistently across global vs manual instrumentors ([opentelemetry-python-contrib.readthedocs.io][17])

---

## F) Logging: gaps between “correlation injection” and “logs signal export”

### F1) `opentelemetry-instrumentation-logging` (correlation injection) deeper controls

I described correlation injection, but didn’t deeply cover:

* `OTEL_PYTHON_LOG_FORMAT` and `OTEL_PYTHON_LOG_LEVEL` semantics and ordering constraints (basicConfig already called → no effect)
* programmatic `LoggingInstrumentor(set_logging_format=True)` vs env var configuration ([opentelemetry-python-contrib.readthedocs.io][18])

### F2) Logs SDK pipeline (LoggerProvider + LoggingHandler + exporters)

I only lightly mentioned logs; I didn’t provide a full deep dive into:

* using `LoggerProvider` + `LoggingHandler`
* BatchLogRecordProcessor vs Console exporters vs OTLP log exporter
* signal maturity considerations (“logs API & SDK under development”) ([OpenTelemetry][19])

### F3) Logs auto-instrumentation via agent

I mentioned it exists but did not detail how `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED` changes behavior (root logger handler attachment; interactions with structured logging setups). ([OpenTelemetry][1])

---

## G) Async & concurrency: gaps

### G1) Asyncio instrumentation semantics

I noted coroutine-name selection and metrics; I did not deeply cover:

* what constitutes an “asyncio process” for the emitted metrics (`asyncio.process.duration`, `asyncio.process.count`)
* how future tracing and to_thread tracing interact with context propagation expectations ([PyPI][20])

### G2) Threading propagation boundaries

Threading instrumentation propagates context into `threading.Thread`, `threading.Timer`, and `ThreadPoolExecutor` worker threads; I didn’t deeply cover:

* limitations (e.g., **process pools** are not in scope here)
* “what happens when spans are created before/after instrumentation”
* failure modes when libraries create threads early ([opentelemetry-python-contrib.readthedocs.io][21])

### G3) Manual context propagation patterns

I didn’t expand manual `propagate.inject/extract` usage for:

* subprocess boundaries
* message queues / job schedulers
* controlling propagation to external/public endpoints (security) ([OpenTelemetry][22])

---

## H) System metrics instrumentation: gaps

### H1) Mapping to semconv + stability + “recommended vs opt-in”

I listed default metrics but didn’t deeply cover:

* how the instrumentation’s metric set aligns to the evolving system/process semconv (status: Development)
* which metrics are “recommended” vs “opt-in” (e.g., `process.cpu.utilization` is opt-in in semconv)
* how to handle breaking changes while system semconv remains unstable ([opentelemetry-python-contrib.readthedocs.io][23])

### H2) Labeling strategy

The instrumentor allows labels and configurable sub-measures; I didn’t cover:

* label cardinality constraints
* recommended label sets for “fleet” vs “single box” observability
* how to avoid exploding metrics with host/network/disk dimensions ([opentelemetry-python-contrib.readthedocs.io][23])

---

## I) DB-API instrumentation: gaps

### I1) Sensitive-data posture and capture strategy

I noted it exists, but didn’t deeply cover:

* `capture_parameters` and why `db.statement.parameters` can be a major PII/secret leak vector
* how this interacts with DB semantic conventions and redaction policy ([opentelemetry-python-contrib.readthedocs.io][24])

### I2) sqlcommenter & attribute commenter

I did not cover in depth:

* `enable_commenter` / `commenter_options`
* `enable_attribute_commenter` (sqlcomment included in `db.statement` span attribute)
* duplication pitfalls when multiple layers add sqlcommenter ([opentelemetry-python-contrib.readthedocs.io][24])

### I3) Semconv stability migration for DB

I didn’t cover how DB instrumentations are expected to support `OTEL_SEMCONV_STABILITY_OPT_IN=database` / `database/dup` and what attribute/metric changes that implies. ([OpenTelemetry][5])

---

## J) Suppression / recursion prevention (SDK ↔ instrumentation): gaps

I referenced “avoid double instrumentation” at the library layer, but I did **not** deeply cover the more fundamental mechanism: **suppression of instrumentation inside exporters** (prevent the SDK’s own export HTTP calls from generating spans/logs).

* There is a well-known suppression key used in context (`_SUPPRESS_INSTRUMENTATION_KEY`) and historical coupling between SDK and instrumentation utilities. This is important if you build custom exporters/processors or do unusual in-process exporting. ([DeepWiki][25])

---

# The highest-impact “missing deep dives” to do next (if you want them)

1. **Semconv stability + schema URL** playbook (http/database/rpc): what changes, how to roll out `*/dup`, how to update dashboards/alerts. ([OpenTelemetry][4])
2. **Exact emitted telemetry surfaces** per instrumentation: spans + attributes + metrics + units + bucket boundaries (HTTP + RPC especially). ([OpenTelemetry][9])
3. **Sensitive data controls** (headers, db params, URLs): allowlists, sanitization, and where the spec explicitly warns you. ([OpenTelemetry][26])
4. **Logs: correlation vs logs-signal export** and how agent auto-log instrumentation interacts with your structured logging. ([OpenTelemetry][1])

If you tell me which of those you want first, I’ll expand it in the same dense “LLM-agent” style as the earlier deep dives.

[1]: https://opentelemetry.io/docs/zero-code/python/configuration/ "Agent Configuration | OpenTelemetry"
[2]: https://opentelemetry.website.cncfstack.com/docs/specs/otel/configuration/sdk-environment-variables/?utm_source=chatgpt.com "Environment Variable Specification | OpenTelemetry"
[3]: https://opentelemetry.io/docs/zero-code/python/example/?utm_source=chatgpt.com "Auto-Instrumentation Example | OpenTelemetry"
[4]: https://opentelemetry.io/docs/specs/semconv/non-normative/http-migration/?utm_source=chatgpt.com "HTTP semantic convention stability migration | OpenTelemetry"
[5]: https://opentelemetry.io/docs/specs/semconv/non-normative/db-migration/?utm_source=chatgpt.com "Database semantic convention stability migration guide | OpenTelemetry"
[6]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/?utm_source=chatgpt.com "Semantic conventions for RPC spans | OpenTelemetry"
[7]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/urllib3.html?utm_source=chatgpt.com "opentelemetry.instrumentation.urllib3 — OpenTelemetry Python Contrib documentation"
[8]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/fastapi.html?utm_source=chatgpt.com "opentelemetry.instrumentation.fastapi — OpenTelemetry Python Contrib documentation"
[9]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/?utm_source=chatgpt.com "Semantic conventions for HTTP metrics | OpenTelemetry"
[10]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/asgi.html?utm_source=chatgpt.com "opentelemetry.instrumentation.asgi — OpenTelemetry Python Contrib documentation"
[11]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/asgi/asgi.html?utm_source=chatgpt.com "OpenTelemetry ASGI Instrumentation — OpenTelemetry Python Contrib documentation"
[12]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/httpx/httpx.html?utm_source=chatgpt.com "OpenTelemetry HTTPX Instrumentation — OpenTelemetry Python Contrib documentation"
[13]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/aiohttp_client/aiohttp_client.html?utm_source=chatgpt.com "OpenTelemetry aiohttp client Instrumentation — OpenTelemetry Python Contrib documentation"
[14]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/requests/requests.html?utm_source=chatgpt.com "OpenTelemetry requests Instrumentation — OpenTelemetry Python Contrib documentation"
[15]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/requests.html?utm_source=chatgpt.com "opentelemetry.instrumentation.requests — OpenTelemetry Python Contrib documentation"
[16]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/?utm_source=chatgpt.com "Semantic conventions for RPC metrics | OpenTelemetry"
[17]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/grpc/grpc.html?utm_source=chatgpt.com "OpenTelemetry gRPC Instrumentation — OpenTelemetry Python Contrib documentation"
[18]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html?utm_source=chatgpt.com "OpenTelemetry Logging Instrumentation — OpenTelemetry Python Contrib documentation"
[19]: https://opentelemetry.io/docs/languages/python/instrumentation/?utm_source=chatgpt.com "Instrumentation | OpenTelemetry"
[20]: https://pypi.org/project/opentelemetry-instrumentation-asyncio/?utm_source=chatgpt.com "opentelemetry-instrumentation-asyncio · PyPI"
[21]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/threading/threading.html?utm_source=chatgpt.com "OpenTelemetry Threading Instrumentation — OpenTelemetry Python Contrib documentation"
[22]: https://opentelemetry.io/docs/concepts/context-propagation/?utm_source=chatgpt.com "Context propagation | OpenTelemetry"
[23]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/system_metrics/system_metrics.html?utm_source=chatgpt.com "OpenTelemetry system metrics Instrumentation — OpenTelemetry Python Contrib documentation"
[24]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/dbapi/dbapi.html?utm_source=chatgpt.com "OpenTelemetry Database API Instrumentation — OpenTelemetry Python Contrib documentation"
[25]: https://deepwiki.com/open-telemetry/opentelemetry-python/8.1-testing?utm_source=chatgpt.com "Context Management | open-telemetry/opentelemetry-python | DeepWiki"
[26]: https://opentelemetry.io/docs/specs/semconv/registry/attributes/http/?utm_source=chatgpt.com "HTTP | OpenTelemetry"

Below is an **LLM-agent–oriented “semconv stability + schema URL” playbook** for **HTTP / Database / RPC** migrations: what changes, what `*/dup` actually does, and a concrete rollout strategy that keeps **dashboards/alerts** intact.

---

## 1) The migration mechanism: `OTEL_SEMCONV_STABILITY_OPT_IN` and why it exists

### 1.1 The invariant OpenTelemetry enforces

When a semantic convention group moves from **experimental → initial stable**, OpenTelemetry requires instrumentations to ship a *graceful migration path*:

* Instrumentations **SHOULD NOT change** what they emit “by default” in an existing major version.
* They **SHOULD introduce** `OTEL_SEMCONV_STABILITY_OPT_IN` to let users opt into stable conventions, or into **dual-emission** (`*/dup`) for phased rollouts.
* They should keep the “dual-emission” major version maintained for at least **six months**.
* In a **next major version**, they may drop the env var and emit only stable. ([OpenTelemetry][1])

Also: the env var is **only intended** for the “experimental → initial stable” migration (not a generic feature flag forever). ([OpenTelemetry][1])

### 1.2 Categories and precedence (the thing agents mess up most often)

`OTEL_SEMCONV_STABILITY_OPT_IN` is a **comma-separated list** of category values (e.g., `http`, `database`, `rpc`). ([OpenTelemetry][2])

For each category:

* `http` / `database` / `rpc` ⇒ emit **stable** conventions only.
* `http/dup` / `database/dup` / `rpc/dup` ⇒ emit **both old + stable** conventions during the transition. ([OpenTelemetry][1])
* `*/dup` has **higher precedence** than `*` if both are present. ([OpenTelemetry][3])

> Operational implication: `*/dup` can increase telemetry volume and (for metrics) effectively create “parallel universes” of names/attributes you must reconcile.

---

## 2) Telemetry Schemas + `schema_url`: the *spec-level* solution to semconv churn

### 2.1 What a telemetry schema is (and what it is not)

OpenTelemetry explicitly recognizes that consumers (dashboards/alerts/backends) make assumptions about attribute/metric names (“telemetry shape”). Telemetry schemas exist to make semconv evolution survivable by defining **explicit transformations** between schema versions when possible. ([OpenTelemetry][4])

### 2.2 Schema URLs: identity + retrieval + immutability

* A **Schema URL** identifies a schema version and is a retrievable `http[s]://.../<version>` URL (version at end of path). ([OpenTelemetry][5])
* Schema files are intended to be **immutable once published** and safe to cache. ([OpenTelemetry][4])
* OpenTelemetry publishes its schema at `opentelemetry.io/schemas/<version>`, and the schema version matches the **semantic conventions** version. ([OpenTelemetry][5])

### 2.3 Where `schema_url` lives in OTLP (and precedence rules)

OTLP allows schema URLs on both resource-level and scope-level envelopes:

* `schema_url` exists on `ResourceSpans/ResourceMetrics/ResourceLogs` **and** on `InstrumentationLibrary*` (“instrumentation scope” layer).
* If both are set, the **InstrumentationLibrary* schema_url takes precedence**. ([OpenTelemetry][5])

### 2.4 Why you should care even if your backend ignores `schema_url`

The spec expectation is: telemetry sources emit schema URL; consumers “pay attention” and may transform telemetry from the received schema to the schema expected by a dashboard/alert. ([OpenTelemetry][5])

Even if your vendor doesn’t implement schema translation today, you can still use this model internally:

* treat `/dup` as a **temporary compatibility mode**
* optionally use your Collector to implement *practical transforms* (rename attributes, drop legacy fields) because Collector processors are designed for filtering/renaming/recalculating telemetry, and order matters. ([OpenTelemetry][6])

---

## 3) HTTP semconv stabilization: what changes (spans + metrics) and why dashboards break

OpenTelemetry’s HTTP migration guide summarizes the breaking changes from the old experimental set to stable. ([OpenTelemetry][1])

### 3.1 Attribute/key renames you must account for

Common + client/server shifts include:

* `http.method` → `http.request.method` (now “known methods” only; unknown becomes `_OTHER`)
* `http.status_code` → `http.response.status_code`
* network peer identity changes:

  * `net.peer.name` → `server.address`
  * `net.peer.port` → `server.port`
* protocol naming:

  * `net.protocol.name` → `network.protocol.name`
  * `net.protocol.version` → `network.protocol.version`
* some network attrs removed (e.g., `net.sock.peer.addr`) ([OpenTelemetry][7])

Also:

* Client: `http.url` → `url.full`, `http.resend_count` → `http.request.resend_count` ([OpenTelemetry][7])

### 3.2 Span naming changes (traces UIs break here too)

Span naming rule changes: when `{http.method}` is `_OTHER`, the `{http.method}` portion of the span name becomes `HTTP`. ([OpenTelemetry][7])
If you had trace search rules or service maps keyed on old span-name formats, they’ll drift.

### 3.3 Metric name + unit changes (alerts almost always break here)

The migration guide explicitly calls out the core latency metrics rename + unit change:

* `http.client.duration` → `http.client.request.duration`
* `http.server.duration` → `http.server.request.duration`
* Unit `ms` → `s`
* Histogram bucket boundaries updated accordingly (and “zero bucket boundary removed”) ([OpenTelemetry][7])

The stable HTTP metrics spec also **hard-recommends** explicit bucket boundaries for the duration histograms, in seconds:
`[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`. ([OpenTelemetry][3])

### 3.4 “Known methods” and `*_original` fallbacks (dashboards must handle `_OTHER`)

Stable HTTP rules:

* `http.request.method` MUST be `_OTHER` when the method isn’t “known” to instrumentation.
* If instrumentation canonicalizes case-insensitive methods, it MUST set `http.request.method_original`. ([OpenTelemetry][3])
  There’s also a mandated override mechanism: `OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS` (full override list). ([OpenTelemetry][3])

**Dashboard impact:** if you group by method, you’ll suddenly see `_OTHER` unless you ensure your known-method list matches reality (or use `*_original` where appropriate).

### 3.5 `error.type` becomes part of the stable story

Stable HTTP conventions add/standardize `error.type` handling; guidance says it should be an exception type or a low-cardinality identifier; if status code indicates an error, `error.type` may be the status code number as string (or exception/identifier). ([OpenTelemetry][3])

**Alert impact:** error-rate alerts keyed on prior fields (or only on status code) may miss errors that are now captured differently unless updated.

---

## 4) Database semconv stabilization: key changes + the “query text safety” cliff

Database migration (v1.24 → v1.33 in the guide) includes significant attribute reshaping and new metrics. ([OpenTelemetry][8])

### 4.1 Attribute removals/renames (spans + logs queries break)

Major changes include:

* Removed: `db.connection_string`, `db.user`, `network.transport`, `network.type`
* `db.name` removed → integrated into new `db.namespace`
* `db.system` → `db.system.name`
* `db.statement` → `db.query.text` **with explicit sanitization warning** (“SHOULD be collected by default only if there is sanitization that excludes sensitive information”)
* `db.operation` → `db.operation.name`
* collection/table naming unification:

  * `db.sql.table` / `db.cassandra.table` / `db.mongodb.collection` / etc. → `db.collection.name`
* New: `db.query.summary`, `db.operation.batch.size`, `db.response.status_code`, `db.stored_procedure.name`, `error.type`
* New (not marked stable yet): `db.operation.parameter.<key>`, `db.query.parameter.<key>`, `db.response.returned_rows` ([OpenTelemetry][9])

**Security/ops implication:** migrating from `db.statement` to `db.query.text` is *not just a rename*; it forces you to decide whether you can sanitize safely or must avoid capturing full query text.

### 4.2 Span naming recommendations changed

The guide explicitly notes: “The recommended span name has changed” (v1.33.0) relative to older versions. ([OpenTelemetry][9])
If you built trace UI views keyed on span names for DB operations, you must re-baseline.

### 4.3 New required metric: `db.client.operation.duration`

The guide states:

* `db.client.operation.duration` is **required**
* “There was no similar metric previously.” ([OpenTelemetry][9])

**Alert impact:** if you previously used only spans for DB latency SLOs, you now have a canonical metric to drive alerts; conversely, any attempt to “compare old vs new” must recognize there was no direct predecessor.

### 4.4 Connection pool metrics rename + unit changes (if your stack emits them)

The migration guide enumerates experimental connection metric renames like:

* `db.client.connections.usage` → `db.client.connection.count`
* `pool.name` → `db.client.connection.pool.name`
* `state` → `db.client.connection.state`
  and multiple time metrics switch `ms` → `s` (e.g., `create_time`, `wait_time`, `use_time`). ([OpenTelemetry][9])

**Dashboards break** exactly the same way as HTTP: metric rename + unit change + label rename.

---

## 5) RPC semconv stabilization: current status, what changes matter, and cardinality traps

### 5.1 Status and migration mechanism

RPC semconv is currently marked **Development**, but it already specifies the same `OTEL_SEMCONV_STABILITY_OPT_IN` migration model:

* `rpc` vs `rpc/dup`
* `rpc/dup` higher precedence
* six-month dual-support guidance ([OpenTelemetry][2])

There’s an explicit RPC stabilization project announcement describing the intent to ship new conventions behind an opt-in flag and maintain both versions for an extended period. ([OpenTelemetry][10])

### 5.2 Span naming rules (your trace UI queries need these)

RPC spans:

* Span name SHOULD be `{rpc.method}` if available and not `_OTHER`
* else span name SHOULD be `{rpc.system.name}` ([OpenTelemetry][11])

### 5.3 The `_OTHER` + `rpc.method_original` rule (cardinality control)

RPC method can have unbounded cardinality in edge/error cases, so the spec mandates:

* if method not recognized → set `rpc.method = _OTHER`
* and MUST also set `rpc.method_original` to the original value
* should provide a way to configure the list of recognized methods if valid methods could be converted to `_OTHER`. ([OpenTelemetry][11])

**Dashboard impact:** if you group by method naïvely, you’ll either explode cardinality (if you use original methods) or lose detail (if you ignore `rpc.method_original`). You must decide where on that spectrum you want to land.

### 5.4 RPC metrics names and bucket boundaries (alerts must align)

RPC metrics spec defines canonical histograms, including:

* `rpc.client.call.duration` (recommended)
* `rpc.server.call.duration` (recommended)
  …with the same explicit bucket boundaries pattern in seconds:
  `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`. ([OpenTelemetry][12])

Key metric attributes include:

* `rpc.system.name`, `rpc.method`, `rpc.response.status_code`, `error.type`
* plus `server.address`, `server.port`, `network.protocol.name` (as available) ([OpenTelemetry][12])

**Alert impact:** if you want reliable error alerts, do not rely only on status code; incorporate `error.type` (which is Stable even when other RPC pieces are Development). ([OpenTelemetry][12])

---

## 6) Rollout playbook for `*/dup` + dashboards/alerts (HTTP / DB / RPC)

This is the safest operational sequence that aligns with how OTel expects you to migrate.

### Phase 0 — Inventory the “contract surface”

Build an explicit inventory of everything downstream that depends on names:

* dashboards: metric names, label keys, span names, attribute keys
* alerts/SLOs: threshold units (ms vs s), histogram bucket queries, error filters
* saved searches and trace UI filters

Treat this as your “telemetry consumer contract” (the exact problem telemetry schemas are meant to solve). ([OpenTelemetry][4])

### Phase 1 — Upgrade instrumentations but keep defaults (no behavior change)

Upgrade to instrumentation versions that implement stable semconv support, but **do not flip opt-in yet** so you don’t break production consumers prematurely. (This matches the “don’t change defaults in the same major version” principle.) ([OpenTelemetry][1])

### Phase 2 — Enable `*/dup` on a canary slice (dual-emission)

Turn on dual-emission for the domain you’re migrating:

Example:

* `OTEL_SEMCONV_STABILITY_OPT_IN=http/dup`
* later `...,database/dup` etc

Remember precedence: `http/dup` > `http`, same for `rpc/dup`. ([OpenTelemetry][3])

**Expected effects you must plan for:**

* metrics: **new metric names appear alongside old** (e.g., HTTP duration metric rename) and units differ (ms→s). ([OpenTelemetry][7])
* spans: attribute keysets expand; some span names change when `_OTHER` is used. ([OpenTelemetry][7])

### Phase 3 — Update dashboards/alerts using a “dual-read” strategy

You have three practical patterns:

#### Pattern A: Dual dashboards / dual queries (most robust)

For each dashboard/alert, create a new version that reads stable names *and* validates against the legacy one during the canary window.

Concrete update examples:

**HTTP latency (metric + units)**

* old: `http.server.duration` in `ms`
* new: `http.server.request.duration` in `s` ([OpenTelemetry][7])
  Update:
* rename metric
* convert thresholds (e.g., 250ms → 0.25s)
* ensure your histogram queries assume the stable bucket boundaries scale. ([OpenTelemetry][3])

**HTTP labels**

* `http.method` → `http.request.method`
* `http.status_code` → `http.response.status_code`
* and decide how to handle `_OTHER` + `http.request.method_original` (especially if your framework canonicalizes). ([OpenTelemetry][1])

**DB spans**

* update `db.system` → `db.system.name`
* `db.operation` → `db.operation.name`
* decide whether you will index/search on `db.query.text` at all (and if so, your sanitization policy). ([OpenTelemetry][9])

**RPC method handling**

* update span name filters: prefer `{rpc.method}` unless `_OTHER`, else `{rpc.system.name}` ([OpenTelemetry][11])
* group by `rpc.method` but explicitly track `_OTHER` volume; optionally add a “drilldown” view using `rpc.method_original` only in controlled contexts. ([OpenTelemetry][11])

#### Pattern B: Collector translation layer (single consumer contract)

If your backend/dashboards can’t easily do dual-read, use Collector processors to normalize telemetry.
Collector processors are explicitly designed to rename/drop/recalculate telemetry and order matters. ([OpenTelemetry][6])

This is the pragmatic stand-in for “schema translation” when your backend doesn’t support schema_url transformations directly.

#### Pattern C: Recording rules / derived metrics (backend-specific)

Some backends let you define a derived metric “stable_http_server_duration_seconds” as a wrapper around old or new names. This can reduce dashboard churn, but it’s vendor-specific and can hide divergence bugs—use only if you already have a mature rules layer.

### Phase 4 — Flip from `*/dup` to stable-only (`http`, `database`, `rpc`)

Once **all** dashboards/alerts/searches have been validated under dual-emission, switch:

* `OTEL_SEMCONV_STABILITY_OPT_IN=http` (drop dup)
* later `database`, `rpc` ([OpenTelemetry][1])

Maintain “legacy consumers” for a short deprecation window, then delete.

### Phase 5 — Enforce cardinality + safety policies

During and after migration:

* HTTP: decide your `OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS` policy to minimize `_OTHER` while avoiding method-cardinality blowups. ([OpenTelemetry][3])
* RPC: decide your recognized-method configuration and whether you ever expose `rpc.method_original` outside controlled drilldowns. ([OpenTelemetry][11])
* DB: decide whether `db.query.text` is allowed, and what sanitization guarantees you can uphold. ([OpenTelemetry][9])

---

## 7) What to hand an LLM implementation agent as “deployment constraints”

If you want this to be automatable in CodeAnatomy, the agent should treat the following as hard requirements derived from the spec:

1. Do not flip semconv defaults abruptly; use `OTEL_SEMCONV_STABILITY_OPT_IN` and stage with `*/dup`. ([OpenTelemetry][1])
2. Update consumers for **metric renames + unit changes** (HTTP ms→s; DB pool time metrics ms→s). ([OpenTelemetry][7])
3. Handle `_OTHER` + `*_original` patterns for **method cardinality** (HTTP and RPC). ([OpenTelemetry][3])
4. Treat `db.query.text` as sensitive unless sanitization is guaranteed. ([OpenTelemetry][9])
5. Prefer stable fields (`error.type`) as your cross-domain error classifier (HTTP/DB/RPC all converge on it). ([OpenTelemetry][3])
6. If you need normalization, the Collector is the sanctioned place to rename/drop/transform signals (processors, ordered pipelines). ([OpenTelemetry][6])
7. Where possible, keep track of schema versions via `schema_url` concepts (even if your backend doesn’t fully honor it yet). ([OpenTelemetry][5])

---


[1]: https://opentelemetry.io/docs/specs/semconv/non-normative/http-migration/?utm_source=chatgpt.com "HTTP semantic convention stability migration | OpenTelemetry"
[2]: https://opentelemetry.io/docs/specs/semconv/rpc/?utm_source=chatgpt.com "Semantic conventions for RPC | OpenTelemetry"
[3]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/?utm_source=chatgpt.com "Semantic conventions for HTTP metrics | OpenTelemetry"
[4]: https://opentelemetry.io/docs/reference/specification/schemas/?utm_source=chatgpt.com "Telemetry Schemas | OpenTelemetry"
[5]: https://opentelemetry.io/docs/reference/specification/schemas/ "Telemetry Schemas | OpenTelemetry"
[6]: https://opentelemetry.io/docs/collector/configuration/?utm_source=chatgpt.com "Configuration | OpenTelemetry"
[7]: https://opentelemetry.io/docs/specs/semconv/non-normative/http-migration/ "HTTP semantic convention stability migration | OpenTelemetry"
[8]: https://opentelemetry.io/docs/specs/semconv/non-normative/db-migration/?utm_source=chatgpt.com "Database semantic convention stability migration guide | OpenTelemetry"
[9]: https://opentelemetry.io/docs/specs/semconv/non-normative/db-migration/ "Database semantic convention stability migration guide | OpenTelemetry"
[10]: https://opentelemetry.io/blog/2025/stabilizing-rpc-conventions/?utm_source=chatgpt.com "Announcing the RPC Semantic Conventions stabilization project | OpenTelemetry"
[11]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/ "Semantic conventions for RPC spans | OpenTelemetry"
[12]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/ "Semantic conventions for RPC metrics | OpenTelemetry"

Below is an “**exact telemetry surface**” deep dive for the **HTTP + RPC** instrumentations you listed—focused on what an implementation agent must assume will be **emitted** (spans/attrs/metrics/units/buckets), and where emission differs by **semconv stability mode**.

---

## 0) Baseline contract: semconv mode drives names, units, and attribute sets

### 0.1 HTTP spans (trace signal): stable naming + required/conditional attrs

The **HTTP span semantic conventions** define:

* **Span name** pattern: `{method} {target}` (target should be `http.route` for server spans; do not default to raw URI path as target). If `http.request.method` is `_OTHER`, `{method}` MUST be `HTTP`. ([OpenTelemetry][1])
* Method canonicalization: unknown methods become `_OTHER`, and instrumentations MUST provide an override list via `OTEL_INSTRUMENTATION_HTTP_KNOWN_METHODS` if valid methods might be coerced to `_OTHER`. ([OpenTelemetry][1])
* Status and error semantics: span status and `error.type` rules are specified based on HTTP status code ranges and failures before status is available. ([OpenTelemetry][1])

### 0.2 HTTP metrics: canonical stable metric names + explicit histogram boundaries (seconds)

The stable HTTP metrics semconv defines required histograms:

* **Server**: `http.server.request.duration` (unit `s`)
* **Client**: `http.client.request.duration` (unit `s`)
* Both SHOULD use **ExplicitBucketBoundaries**:
  `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`. ([OpenTelemetry][2])

### 0.3 RPC metrics (including gRPC as an RPC system): canonical histogram boundaries (seconds)

The RPC metrics semconv defines:

* `rpc.client.call.duration` and `rpc.server.call.duration` histograms (unit `s`) with the same recommended boundaries:
  `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`. ([OpenTelemetry][3])

### 0.4 RPC spans + gRPC overrides

RPC span conventions require:

* **Span kind**: CLIENT for outbound, SERVER for inbound
* **Span name**: `rpc.method` (if available and not `_OTHER`), else `rpc.system.name` ([OpenTelemetry][4])
  gRPC-specific semconv extends RPC and mandates:
* `rpc.system.name = "grpc"`
* `rpc.method` required
* `rpc.response.status_code` required on server spans; also defines which gRPC status codes are considered errors and how `error.type` should be set. ([OpenTelemetry][5])

---

## 1) HTTP Server (ASGI core): `opentelemetry-instrumentation-asgi`

Treat ASGI’s `OpenTelemetryMiddleware` as the **canonical emission surface** for FastAPI/Starlette too, because those wrappers ultimately route through this middleware.

### 1.1 Spans emitted (topology)

**A) One “server” span per request (HTTP + WebSocket)**

* Span name defaults to `"{method} {path}"` for HTTP (method sanitized, `_OTHER`→`HTTP`), `"{path}"` for websockets, or just method if no path. ([opentelemetry-python-contrib.readthedocs.io][6])
* Span is created via `_start_internal_or_server_span(...)`, which extracts incoming context from ASGI headers and decides whether it is a SERVER span (typical inbound request) or an INTERNAL span (if appropriate). ([opentelemetry-python-contrib.readthedocs.io][6])

**B) Optional “internal” spans around ASGI `receive` and `send`**
Unless excluded, middleware creates:

* `"<server_span_name> <scope[type]> receive"`
* `"<server_span_name> <scope[type]> send"`
  Each internal span gets an `asgi.event.type` attribute, and status code is set for certain message types (e.g., websocket send). ([opentelemetry-python-contrib.readthedocs.io][6])

**Noise control**: `exclude_spans` can disable receive and/or send spans. ([opentelemetry-python-contrib.readthedocs.io][6])

### 1.2 Span attributes emitted (server span)

The middleware constructs request attributes in `collect_request_attributes(...)`. Emission is **semconv-mode aware** (old vs new) via `_set_http_*` helpers and `_report_old/_report_new`. ([opentelemetry-python-contrib.readthedocs.io][6])

Concrete attribute surfaces observed in code:

**Always/commonly set from ASGI scope**

* Scheme → `_set_http_scheme(...)` ([opentelemetry-python-contrib.readthedocs.io][6])
* Host/server name → `_set_http_host_server(...)` and (legacy) `SpanAttributes.HTTP_SERVER_NAME` derived from `Host` header when old semconv is active ([opentelemetry-python-contrib.readthedocs.io][6])
* Server port → `_set_http_net_host_port(...)` ([opentelemetry-python-contrib.readthedocs.io][6])
* HTTP version (“flavor”) → `_set_http_flavor_version(...)` ([opentelemetry-python-contrib.readthedocs.io][6])
* Target/path/query → `_set_http_target(...)` (and it may be updated later to a **parameterized target** for metrics; see §1.4) ([opentelemetry-python-contrib.readthedocs.io][6])
* URL: in old mode, URL is recorded via `_set_http_url(..., redact_url(http_url), DEFAULT)` ([opentelemetry-python-contrib.readthedocs.io][6])
* Method: `_set_http_method(...)` with both original + sanitized method ([opentelemetry-python-contrib.readthedocs.io][6])
* User-Agent: `_set_http_user_agent(...)`, plus synthetic UA classification `USER_AGENT_SYNTHETIC_TYPE` if detected ([opentelemetry-python-contrib.readthedocs.io][6])
* Client peer address/port: `_set_http_peer_ip_server(...)`, `_set_http_peer_port_server(...)` when `scope["client"]` is present ([opentelemetry-python-contrib.readthedocs.io][6])

**Response status + error classification**

* On response messages (and on internal send span), `set_status_code(...)` calls `_set_status(...)` which sets response status code attributes and status/error semantics in a mode-aware way. ([opentelemetry-python-contrib.readthedocs.io][6])

**Header capture (opt-in allowlist)**
If enabled, request headers are captured as `http.request.header.<key> = ["..."]` and response headers as `http.response.header.<key> = ["..."]`, with regex matching and sanitize list that replaces values with `[REDACTED]`. ([opentelemetry-python-contrib.readthedocs.io][6])
This matches the HTTP semconv guidance that header capture must be explicitly configured. ([OpenTelemetry][1])

### 1.3 Metrics emitted (names, units, and when they record)

ASGI middleware emits **both legacy and stable** metrics depending on semconv mode.

#### A) Duration histograms (server request duration)

At request completion (HTTP only), ASGI records:

* **Legacy** duration histogram: `duration_histogram_old.record(round(duration_s * 1000), ...)` (milliseconds)
* **Stable** duration histogram: `duration_histogram_new.record(duration_s, ...)` (seconds) ([opentelemetry-python-contrib.readthedocs.io][6])

Initialization shows:

* Stable histogram uses `HTTP_SERVER_REQUEST_DURATION` with unit `"s"` and `explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW`. ([opentelemetry-python-contrib.readthedocs.io][6])
  Stable **spec** expects `http.server.request.duration` with seconds and boundaries `[0.005..10]`. ([OpenTelemetry][2])

#### B) Active requests (in-flight count)

For HTTP requests, middleware increments active requests on entry and decrements on exit using `active_requests_counter.add(+1, ...)` / `.add(-1, ...)`, created via `create_http_server_active_requests(meter)`. ([opentelemetry-python-contrib.readthedocs.io][6])

#### C) Request/response size histograms

On completion, ASGI records:

* Response size from `Content-Length` into:

  * legacy `MetricInstruments.HTTP_SERVER_RESPONSE_SIZE` (unit `By`)
  * stable `create_http_server_response_body_size(...)` (also bytes) ([opentelemetry-python-contrib.readthedocs.io][7])
* Request size from request `Content-Length` into:

  * legacy `MetricInstruments.HTTP_SERVER_REQUEST_SIZE` (unit `By`)
  * stable `create_http_server_request_body_size(...)` ([opentelemetry-python-contrib.readthedocs.io][7])

### 1.4 Metric attribute sets (legacy vs stable) + parameterized target

When recording duration metrics:

* Old attrs are derived by filtering request attrs (`_parse_duration_attrs(attributes, DEFAULT)`) and then **explicitly** include `SpanAttributes.HTTP_TARGET = target` when a parameterized target is available. ([opentelemetry-python-contrib.readthedocs.io][6])
* New attrs are derived by filtering request attrs using stability mode HTTP (`_parse_duration_attrs(attributes, HTTP)`). ([opentelemetry-python-contrib.readthedocs.io][6])

Parameterized target for metrics:

* `_collect_target_attribute(scope)` has FastAPI-specific logic: if `scope["route"].path_format` exists, return `"{root_path}{path_format}"` (e.g., `/api/users/{user_id}`), which is then applied via `_set_http_target(...)` before metric recording. ([opentelemetry-python-contrib.readthedocs.io][6])

This is critical for cardinality control: you get low-cardinality route templates for metrics **when the framework provides it** (FastAPI does).

---

## 2) Server wrappers: FastAPI + Starlette

### 2.1 `opentelemetry-instrumentation-fastapi`

FastAPI instrumentation is principally:

* a convenience wrapper over ASGI middleware configuration (excluded URLs, hooks, header capture lists, receive/send span suppression), and
* additional machinery to handle FastAPI/Starlette error middleware behavior (it imports `ServerErrorMiddleware`). ([opentelemetry-python-contrib.readthedocs.io][8])

It also initializes HTTP semconv stability mode before instrumenting the app. ([opentelemetry-python-contrib.readthedocs.io][8])

**Key takeaway for emitted surfaces**: spans/metrics come from ASGI middleware (§1), with FastAPI contributing:

* access to `scope["route"].path_format` so the ASGI middleware can parameterize the metric target (low-cardinality). ([opentelemetry-python-contrib.readthedocs.io][6])

### 2.2 `opentelemetry-instrumentation-starlette`

Starlette instrumentation similarly focuses on:

* excluded URLs, hooks, header capture, sanitize lists (same env var contract as ASGI/FastAPI). ([opentelemetry-python-contrib.readthedocs.io][9])
* It imports Starlette route matching support (`starlette.routing.Match`) and the incubating constant `HTTP_ROUTE`, indicating route-template derivation is part of its emission strategy. ([opentelemetry-python-contrib.readthedocs.io][9])

**Inference (verify in your version)**: Starlette typically provides a route template for `http.route` and span naming using that route template. The “mechanism” is implied by imported symbols, but the exact assignment site should be verified in the module body for your pinned version. ([opentelemetry-python-contrib.readthedocs.io][9])

---

## 3) HTTP Clients: emitted surfaces per library

All client instrumentations are semconv-mode aware and converge on:

* **CLIENT span** around the request
* **duration histogram** old (`http.client.duration`, ms) vs new (`http.client.request.duration`, s + recommended buckets)
* optional request/response size histograms depending on library

### 3.1 `opentelemetry-instrumentation-httpx`

**Metrics (explicit in code)**

* Old: creates `MetricInstruments.HTTP_CLIENT_DURATION` with unit `"ms"` and explicit bucket boundaries advisory `HTTP_DURATION_HISTOGRAM_BUCKETS_OLD` (legacy).
* New: creates `http.client.request.duration` (`HTTP_CLIENT_REQUEST_DURATION`) with unit `"s"` and `HTTP_DURATION_HISTOGRAM_BUCKETS_NEW`. ([opentelemetry-python-contrib.readthedocs.io][10])
* Recording behavior:

  * old histogram records `round(elapsed_time * 1000)` (ms)
  * new histogram records `elapsed_time` (s) ([opentelemetry-python-contrib.readthedocs.io][10])

**Spans**

* Transport-level wrapping: `HTTPTransport.handle_request` and `AsyncHTTPTransport.handle_async_request` are wrapped so a span covers the request. ([opentelemetry-python-contrib.readthedocs.io][10])

### 3.2 `opentelemetry-instrumentation-urllib3`

This is the most explicit “full-surface” HTTP client implementation.

**Spans (explicit in code)**

* Span name is derived from sanitized method; if `_OTHER`, name becomes `"HTTP"`. ([opentelemetry-python-contrib.readthedocs.io][11])
* A CLIENT span is started with initial attributes including:

  * method via `_set_http_method(...)`
  * URL via `_set_http_url(...)` ([opentelemetry-python-contrib.readthedocs.io][11])

**Metric attributes (explicit in code)**
Before recording, it sets metric attribute keys from connection pool + request:

* `_set_http_host_client(...)`
* `_set_http_scheme(...)`
* `_set_http_method(...)`
* `_set_http_net_peer_name_client(...)`
* `_set_http_peer_port_client(...)` ([opentelemetry-python-contrib.readthedocs.io][11])

**Metrics (explicit in code)**
Legacy (old semconv):

* `http.client.duration` (unit `ms`)
* `http.client.request.size` (unit `By`)
* `http.client.response.size` (unit `By`) ([opentelemetry-python-contrib.readthedocs.io][11])

Stable (new semconv):

* `http.client.request.duration` (unit `s`, `HTTP_DURATION_HISTOGRAM_BUCKETS_NEW`)
* `http.client.request.body.size` (via `create_http_client_request_body_size`)
* `http.client.response.body.size` (via `create_http_client_response_body_size`) ([opentelemetry-python-contrib.readthedocs.io][11])

Recording behavior:

* old duration recorded in ms (`round(duration_s * 1000)`)
* new duration recorded in seconds (`duration_s`)
* request/response size recorded into old/new histograms when available ([opentelemetry-python-contrib.readthedocs.io][11])

### 3.3 `opentelemetry-instrumentation-requests`

**Metrics**

* It supports the same “old vs new” dual histogram approach (implementation uses semconv filtering and records new histogram in seconds). ([opentelemetry-python-contrib.readthedocs.io][12])
* It uniquely supports **custom bucket boundaries** at instrumentation time via `duration_histogram_boundaries=[...]`. ([opentelemetry-python-contrib.readthedocs.io][13])

**Spans**

* Default span naming callback returns `HTTP {method_name}` (per docs in code). ([opentelemetry-python-contrib.readthedocs.io][12])

### 3.4 `opentelemetry-instrumentation-urllib` (stdlib)

Primary sources available here (docs + ecosystem summaries) confirm:

* It instruments stdlib `urllib.request` and supports request/response hooks and excluded URLs. ([opentelemetry-python-contrib.readthedocs.io][14])
* It is part of the “HTTP client instrumentations” set that participates in semconv migration and duration metric old/new split. (Exact internal surfaces should be verified against the repo file for your pinned version; the high-level set matches the other client libs.) ([DeepWiki][15])

### 3.5 `opentelemetry-instrumentation-aiohttp-client`

**Spans**

* The instrumentation explicitly states: “One span is created for the entire HTTP request, including initial TCP/TLS setup if the connection doesn’t exist.” ([opentelemetry-python-contrib.readthedocs.io][16])
* Default span name is set to the HTTP request method. ([opentelemetry-python-contrib.readthedocs.io][16])

It initializes HTTP semconv stability mode at instrumentation time. ([opentelemetry-python-contrib.readthedocs.io][16])

(For metrics: the presence/shape should be confirmed in your pinned version; it follows the same stability-mode scaffolding as other HTTP clients, but you should verify exact metric instruments in the module body.)

---

## 4) RPC / gRPC: spans + (spec) metrics, and what Python contrib emits today

### 4.1 Spec-defined “correct” RPC emission surface (what you should target)

**Spans**

* CLIENT/SERVER spans, span name based on `rpc.method` unless `_OTHER`, else `rpc.system.name`. ([OpenTelemetry][4])
* Common attributes: `rpc.system.name`, `rpc.method`, `rpc.response.status_code` (system-specific semantics), and `error.type` when failed. ([OpenTelemetry][4])

**Metrics**

* `rpc.client.call.duration` and `rpc.server.call.duration` in seconds with explicit boundaries `[0.005..10]`. ([OpenTelemetry][3])

### 4.2 Python `opentelemetry-instrumentation-grpc`: what it actually emits (as surfaced in the module/docs)

From the public module documentation:

* It provides **client/server interceptors** (sync and `grpc.aio`) and global instrumentors (`GrpcInstrumentorClient/Server`, aio variants). ([opentelemetry-python-contrib.readthedocs.io][17])
* It supports **filters** to include/exclude methods and a deployment env var `OTEL_PYTHON_GRPC_EXCLUDED_SERVICES`. ([opentelemetry-python-contrib.readthedocs.io][18])

**Metrics emission**: in the public module source, there is no evidence of metric instrument creation (no meter/histogram surfaces are described in the module-level API/docs excerpts, and metric-related patterns do not appear in the module text we inspected). ([opentelemetry-python-contrib.readthedocs.io][17])

**Practical consequence**

* If you need **gRPC metrics** today, you may rely on **gRPC’s own OpenTelemetry metrics plugin** (library-level), which emits metrics like `grpc.client.call.duration` (unit `s`, with labels like `grpc.method`, `grpc.target`, `grpc.status`). ([gRPC][19])
* Separately, you can still adopt the **RPC semconv metric names** as your “target shape” at the observability layer even if the library emits different names, by translating in the Collector (attributes/transform processors) during rollout.

---

## 5) Agent-grade “what to verify in your pinned versions” checklist

Because some surfaces are **mode-dependent** (and some instrumentations are in migration), an implementation agent should explicitly verify these in CI by running a tiny service/client and capturing emitted telemetry:

1. **HTTP server**: ensure both `http.server.request.duration` (s) and (if enabled) legacy duration appear as expected under `OTEL_SEMCONV_STABILITY_OPT_IN=http` vs `http/dup`. Boundaries should match the stable spec list. ([OpenTelemetry][2])
2. **HTTP client**: ensure the client libs you actually use emit `http.client.request.duration` (s) and that you aren’t double-instrumenting stacked clients (e.g., requests + urllib3).
3. **Route cardinality**: confirm parameterized target/route is present for metrics (FastAPI path_format path); otherwise dashboards will explode. ([opentelemetry-python-contrib.readthedocs.io][6])
4. **gRPC**: confirm whether your Python grpc instrumentation version emits only spans (most likely) and whether you need gRPC’s plugin metrics; align your dashboards either to RPC semconv (`rpc.*`) or gRPC plugin metrics (`grpc.*`) intentionally. ([gRPC][19])


[1]: https://opentelemetry.io/docs/specs/semconv/http/http-spans/?utm_source=chatgpt.com "Semantic conventions for HTTP spans | OpenTelemetry"
[2]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/?utm_source=chatgpt.com "Semantic conventions for HTTP metrics | OpenTelemetry"
[3]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/?utm_source=chatgpt.com "Semantic conventions for RPC metrics | OpenTelemetry"
[4]: https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/?utm_source=chatgpt.com "Semantic conventions for RPC spans | OpenTelemetry"
[5]: https://opentelemetry.io/docs/specs/semconv/rpc/grpc/?utm_source=chatgpt.com "Semantic conventions for gRPC | OpenTelemetry"
[6]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/asgi.html "opentelemetry.instrumentation.asgi — OpenTelemetry Python Contrib  documentation"
[7]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/asgi.html?utm_source=chatgpt.com "opentelemetry.instrumentation.asgi — OpenTelemetry Python Contrib documentation"
[8]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/fastapi.html?utm_source=chatgpt.com "opentelemetry.instrumentation.fastapi — OpenTelemetry Python Contrib documentation"
[9]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/starlette.html "opentelemetry.instrumentation.starlette — OpenTelemetry Python Contrib  documentation"
[10]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/httpx.html?utm_source=chatgpt.com "opentelemetry.instrumentation.httpx — OpenTelemetry Python Contrib documentation"
[11]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/urllib3.html?utm_source=chatgpt.com "opentelemetry.instrumentation.urllib3 — OpenTelemetry Python Contrib documentation"
[12]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/requests.html?utm_source=chatgpt.com "opentelemetry.instrumentation.requests — OpenTelemetry Python Contrib documentation"
[13]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/requests/requests.html?utm_source=chatgpt.com "OpenTelemetry requests Instrumentation — OpenTelemetry Python Contrib documentation"
[14]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/urllib/urllib3.html?utm_source=chatgpt.com "OpenTelemetry urllib Instrumentation — OpenTelemetry Python Contrib documentation"
[15]: https://deepwiki.com/open-telemetry/opentelemetry-python-contrib/4.1-http-client-libraries?utm_source=chatgpt.com "HTTP Client Libraries | open-telemetry/opentelemetry-python-contrib | DeepWiki"
[16]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/aiohttp_client.html?utm_source=chatgpt.com "opentelemetry.instrumentation.aiohttp_client — OpenTelemetry Python Contrib documentation"
[17]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/grpc.html "opentelemetry.instrumentation.grpc — OpenTelemetry Python Contrib  documentation"
[18]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/grpc/grpc.html?utm_source=chatgpt.com "OpenTelemetry gRPC Instrumentation — OpenTelemetry Python Contrib documentation"
[19]: https://grpc.io/docs/guides/opentelemetry-metrics/?utm_source=chatgpt.com "OpenTelemetry Metrics | gRPC"
