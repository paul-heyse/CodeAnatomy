Below is a *single-source-of-truth* field guide to OpenTelemetry (OTel) for Python—what ships in which packages, how it maps to the specification, how to wire it up for traces, metrics, and logs, and what “best-in-class” implementations do in practice. Every major claim is linked to primary or high‑quality secondary sources so you can rely on it as a reference.

---

## 0) What OpenTelemetry is (and is not)

OpenTelemetry is a vendor‑neutral specification plus language SDKs for emitting **traces, metrics, and logs** (the “signals”), along with a protocol (OTLP) and a **Collector** you run to process and export telemetry. In Python:

* `opentelemetry-api` is the *interfaces* your code calls.
* `opentelemetry-sdk` is the reference *implementation* of those APIs (providers, processors, exporters, readers).
* Instrumentations live in the `opentelemetry-python-contrib` repo and are typically installed as `opentelemetry-instrumentation-<lib>`.
* Auto/zero‑code instrumentation is provided by the `opentelemetry-instrument` CLI (a “Python agent”). ([GitHub][1])

Python’s status today (Oct 2025): traces and metrics are stable; logs are *stable in the spec* but Python’s logs APIs in `opentelemetry.sdk._logs` are still marked “experimental” in the Python SDK docs (so expect API churn), even as the spec’s **Logs SDK** is stable. ([OpenTelemetry][2])

---

## 1) Packages & moving parts (Python)

| Purpose                        | Package(s)                                         | Notes                                                                                                                           |
| ------------------------------ | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| API interfaces                 | `opentelemetry-api`                                | No‑op implementations; apps & libs can depend safely. ([GitHub][1])                                                             |
| SDK implementation             | `opentelemetry-sdk`                                | Providers, processors, exporters, readers. ([GitHub][1])                                                                        |
| OTLP exporters                 | `opentelemetry-exporter-otlp`                      | gRPC or HTTP/Protobuf to a Collector; see env vars below. ([OpenTelemetry Python][3])                                           |
| Instrumentations               | `opentelemetry-instrumentation-*`                  | ASGI/FastAPI/Flask/Django, requests/httpx, SQLAlchemy, psycopg2, Celery, Redis, Kafka, etc. ([OpenTelemetry Python Contrib][4]) |
| Auto‑instrumentation           | `opentelemetry-instrument` CLI                     | Enables runtime patching + exporter setup. ([OpenTelemetry][5])                                                                 |
| Distro bundle (optional)       | `opentelemetry-distro`                             | Opinionated bundle + bootstrap tool to install instrumentations. ([SigNoz][6])                                                  |
| Semantic conventions constants | `opentelemetry-semantic-conventions` (or use spec) | Follow HTTP/DB/messaging semconv when naming. ([OpenTelemetry][7])                                                              |

**Instrumentations list:** Browse the live catalog in the OTel Registry and the Python‑contrib docs to see coverage (e.g., FastAPI, ASGI, SQLAlchemy, psycopg2, Redis, Kafka). ([OpenTelemetry][8])

---

## 2) Collector & OTLP: the production backbone

**Best practice:** export from apps → **OTel Collector** (then fan out to backends), not directly to vendors. The Collector handles batching, transformation, redaction, tail‑sampling, and routing. ([OpenTelemetry][9])

* **Protocol & ports:** OTLP/gRPC defaults to **4317**; OTLP/HTTP to **4318** (match your Collector). ([OpenTelemetry][10])
* **When to prefer gRPC vs HTTP:** gRPC generally performs better; HTTP can be simpler behind proxies. ([SigNoz][11])
* **Tail sampling:** do it in the Collector (needs *load‑balancing* so all spans of a trace land on the same instance). ([OpenTelemetry][12])
* **Transform/PII controls:** use the **attributes**, **redaction**, **filter**, and **transform** processors in the Collector. ([OpenTelemetry][13])

---

## 3) Resources & semantic conventions (what to put on the wire)

**Resource attributes** describe *who* emitted the data. Always set:

* `service.name` (required), plus `service.version`, `service.namespace`, `deployment.environment.name` (e.g., `production`, `staging`). You can set them via code or env (see below). ([OpenTelemetry][14])

Use **semantic conventions** for spans/metrics/logs attributes:

* **HTTP:** span names/attributes and required metrics like `http.server.request.duration`. Recent HTTP semconv stabilization introduces new names (see migration). ([OpenTelemetry][15])
* **Databases:** prefer low‑cardinality span names; use `db.system.name`, `db.operation.name`, etc. ([OpenTelemetry][16])

---

## 4) Context propagation

* Default propagators: **W3C Trace Context** (`traceparent`, `tracestate`) + **W3C Baggage**. You can switch/augment with `OTEL_PROPAGATORS` (e.g., `b3`, `b3multi`, `jaeger`) when interoperating. ([W3C][17])
* Python uses `opentelemetry.propagate` to inject/extract across HTTP, messaging, etc., and the set comes from `OTEL_PROPAGATORS`. ([OpenTelemetry Python][18])

**Caution:** Don’t put PII or unbounded values in Baggage—it rides on every hop. Prefer resource attributes or add data in spans/logs. Use Collector redaction if needed. ([OpenTelemetry][13])

---

## 5) Traces in Python

**Core building blocks**

* **TracerProvider + Sampler:** Typical production sampler is `ParentBased(TraceIdRatioBased(p))` for head sampling. Configure via `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` (e.g., `parentbased_traceidratio`, `0.1`). ([OpenTelemetry Python][19])
* **Span processors:** `BatchSpanProcessor` (default for prod) vs `SimpleSpanProcessor` (tests/dev). Batch controls are tunable; flush & shutdown must obey timeouts. ([OpenTelemetry][20])
* **Export:** Prefer OTLP → Collector (`opentelemetry-exporter-otlp`). Configure with `OTEL_EXPORTER_OTLP_*` envs (endpoint, headers, protocol). ([OpenTelemetry Python][3])

**Manual setup (trace‑only, minimal):**

```python
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

resource = Resource.create({
    "service.name": "checkout",
    "service.version": "1.4.2",
    "deployment.environment.name": "production",
})

provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("checkout-api")

with tracer.start_as_current_span("charge"):
    # do work, add attributes/events/status
    pass
```

**Advanced trace features to use**

* **Exception capture:** `span.record_exception(ex)` and set `StatusCode.ERROR`. (OTel Python follows the spec here.) ([GitHub][1])
* **Span links:** model async workflows (e.g., producer/consumer). (Spec feature supported by Python SDK.) ([OpenTelemetry][20])
* **Tail sampling:** implement centrally in the **Collector** (policies by error code, latency, route, etc.). ([OpenTelemetry][12])

**Gotcha—pre‑fork servers (Gunicorn/uWSGI):** `BatchSpanProcessor` is **not fork‑safe**; use fork hooks or initialize providers *after* forking to avoid deadlocks. See Python SDK guidance. ([OpenTelemetry Python][21])

---

## 6) Metrics in Python

**Core building blocks**

* **MeterProvider + readers/exporters:** For push‑based export, use `PeriodicExportingMetricReader` with OTLP exporter. Views allow you to **rename instruments**, **drop attributes**, or **change aggregations/buckets**. ([OpenTelemetry Python][22])
* **Instruments:** `Counter`, `UpDownCounter`, `Histogram`, and observable `Counter/UpDownCounter/Gauge`. Use *units* (UCUM) and descriptions. ([OpenTelemetry Python][23])
* **Temporality & aggregation defaults:** Exporter temporality/aggregation rules are defined in the spec; Python leverages them (Cumulative temporality is common). ([OpenTelemetry][24])

**Example (histogram customization via Views):**

```python
from opentelemetry.sdk.metrics import MeterProvider, View
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics.aggregation import ExplicitBucketHistogramAggregation

resource = Resource.create({"service.name": "checkout"})
reader = PeriodicExportingMetricReader(OTLPMetricExporter())
provider = MeterProvider(resource=resource, metric_readers=[reader],
    views=[View(
        instrument_name="http.server.request.duration",
        aggregation=ExplicitBucketHistogramAggregation([0.005,0.01,0.025,0.05,0.075,0.1,0.25,0.5,0.75,1,2.5,5,7.5,10])
    )]
)
```

Those default HTTP server buckets are explicitly recommended in the HTTP metrics semconv. ([OpenTelemetry][25])

**Span→metrics pipelines:** generate RED metrics from traces using the Collector’s **spanmetrics connector** (or deprecating processor); useful when you have rich tracing and want SLO metrics. ([Go Packages][26])

---

## 7) Logs in Python

Two ways you’ll use logs with OTel Python:

1. **SDK logging bridge** – programmatic use of `LoggerProvider`, `LoggingHandler`, and `BatchLogRecordProcessor` to export to OTLP.
2. **Auto/zero‑code logs** – the OTel Python agent attaches an OTLP handler to the root logger for you. ([OpenTelemetry Python][27])

**Minimal programmatic setup:**

```python
import logging
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

logger_provider = LoggerProvider()
logger_provider.add_log_record_processor(
    BatchLogRecordProcessor(OTLPLogExporter())
)
logging.getLogger().addHandler(LoggingHandler(logger_provider=logger_provider))
logging.getLogger().setLevel(logging.INFO)
logging.info("order accepted", extra={"order_id":"12345"})
```

See the Python SDK docs for `LoggerProvider`/`LoggingHandler` APIs; note the experimental status in SDK docs despite the spec’s Logs SDK being stable. ([OpenTelemetry Python][27])

**Trace–log correlation (high‑leverage):** enable `OTEL_PYTHON_LOG_CORRELATION=true` to inject `trace_id`/`span_id` into log records with the logging instrumentation (various distros/vendors document this). Format via `OTEL_PYTHON_LOG_FORMAT`. ([OpenTelemetry Python Contrib][28])

> **Tip:** Correlation can also be done downstream in the Collector, but enriching at the edge (in your app) tends to be more reliable. ([Datadog Monitoring][29])

---

## 8) Auto‑instrumentation (zero code)

Install the bundle and instrumentations:

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp
opentelemetry-bootstrap --action=install
```

Run your app with the agent and point it at your Collector:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318"   # http/protobuf
export OTEL_SERVICE_NAME="checkout"
opentelemetry-instrument \
  --traces_exporter otlp \
  --metrics_exporter none \
  --logs_exporter otlp \
  python app.py
```

* The `bootstrap` tool inspects installed libraries and installs matching `opentelemetry-instrumentation-*` packages (Flask, FastAPI, SQLAlchemy, requests, etc.). ([SigNoz][6])
* You can disable specific instrumentations with `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS="redis,kafka-python"`. ([OpenTelemetry][30])

**Framework highlights:**

* **ASGI/FastAPI** have first‑class instrumentations (middleware). Auto‑instrumentation handles them; you can also plug the middleware manually. ([OpenTelemetry Python Contrib][31])
* **SQLAlchemy/psycopg2** instrumentations capture DB spans (mind span naming/cardinality per semconv). ([OpenTelemetry Python Contrib][32])

---

## 9) Environment variable cheat sheet (Python‑relevant)

> The spec defines a stable, cross‑language set. Python’s auto‑instrumentation also defines a few Python‑specific toggles.

**Exporters & endpoints**

* `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` = `otlp|console|prometheus|zipkin|jaeger|none` (language‑dependent availability). Default is `otlp`. ([OpenTelemetry][33])
* `OTEL_EXPORTER_OTLP_ENDPOINT` (and signal‑specific `..._TRACES_ENDPOINT`, etc.), `OTEL_EXPORTER_OTLP_PROTOCOL` = `grpc|http/protobuf`, `..._HEADERS`, `..._TIMEOUT`. ([OpenTelemetry][34])

**Resources**

* `OTEL_SERVICE_NAME` (shortcut) and `OTEL_RESOURCE_ATTRIBUTES="service.name=...,service.version=...,deployment.environment.name=..."`. ([OpenTelemetry][14])

**Propagation**

* `OTEL_PROPAGATORS="tracecontext,baggage"` (default). Options include `b3`, `b3multi`, `jaeger`, `xray`. ([OpenTelemetry][33])

**Sampling**

* `OTEL_TRACES_SAMPLER="parentbased_traceidratio|traceidratio|always_on|always_off|..."`
* `OTEL_TRACES_SAMPLER_ARG="0.1"` (for ratio‑based). ([OpenTelemetry][33])

**Auto‑instrumentation (Python)**

* `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS="requests,django,..."`. ([OpenTelemetry][30])
* `OTEL_PYTHON_LOG_CORRELATION=true` (inject trace/span IDs into logs). ([OpenTelemetry Python Contrib][28])

**Semantic convention migration**

* `OTEL_SEMCONV_STABILITY_OPT_IN=http|http/dup` to opt into the stabilized HTTP/networking semconv during transitions. ([OpenTelemetry][35])

---

## 10) “Best‑in‑class” implementation patterns

1. **Emit to a local/sidecar/daemonset Collector using OTLP** (gRPC when possible). This gives you transformation, governance, and backend agility without redeploying apps. ([OpenTelemetry][9])
2. **Always set `service.name` & environment** and enrich Resource attributes with cloud/Kubernetes context (use resource detectors or the Collector’s **k8sattributes** processor). ([OpenTelemetry][14])
3. **Control cardinality:**

   * Avoid putting IDs/URLs with query strings in attributes.
   * Use **Views** to drop/rename attributes and set histogram buckets. ([OpenTelemetry Python][22])
4. **Sampling strategy:**

   * Default head sampling with `parentbased_traceidratio` for cost control.
   * Add **tail sampling** in the Collector for errors/latency outliers. Ensure trace‑aware load balancing. ([OpenTelemetry][12])
5. **Correlate logs↔traces** at the edge (`OTEL_PYTHON_LOG_CORRELATION=true`) so log lines carry `trace_id`/`span_id`. ([OpenTelemetry Python Contrib][28])
6. **HTTP semconv migration:** Opt into stable HTTP attributes with `OTEL_SEMCONV_STABILITY_OPT_IN` and update dashboards/alerts. ([OpenTelemetry][35])
7. **Shutdown/force‑flush:** On graceful shutdown, call providers’ `shutdown()` or `force_flush()` to prevent data loss (functions must finish within a timeout per spec). ([OpenTelemetry][20])
8. **Pre‑fork servers:** Initialize SDK/components in worker processes or use fork hooks (avoid deadlocks with `BatchSpanProcessor`). ([OpenTelemetry Python][21])
9. **Security & privacy:** enforce redaction in the Collector (attributes/redaction/transform processors). ([OpenTelemetry][13])
10. **Span→metrics:** derive standardized RED metrics from traces via the spanmetrics connector to power SLOs quickly. ([Grafana Labs][36])

---

## 11) Auto‑instrumentation quick recipes

**FastAPI (ASGI)**

```bash
pip install fastapi uvicorn opentelemetry-distro opentelemetry-exporter-otlp
opentelemetry-bootstrap --action=install
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318"
export OTEL_SERVICE_NAME="inventory"
opentelemetry-instrument --traces_exporter otlp --metrics_exporter none \
  uvicorn app:app --host 0.0.0.0 --port 8000
```

ASGI/FastAPI middleware instrumentations are part of Python‑contrib (and picked up by the agent). ([OpenTelemetry Python Contrib][31])

**SQLAlchemy**

```bash
pip install opentelemetry-instrumentation-sqlalchemy
# auto-instrument or manual enable via SQLAlchemyInstrumentor
```

Follow semconv for DB span naming and attributes. ([OpenTelemetry Python Contrib][32])

---

## 12) Manual end‑to‑end (traces + metrics + logs) in one file

> Use this skeleton when you want explicit control instead of the agent.

```python
from opentelemetry.sdk.resources import Resource

# ---- Resource ----
resource = Resource.create({
  "service.name": "payments",
  "service.version": "2.3.0",
  "deployment.environment.name": "staging",
})

# ---- Traces ----
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

tp = TracerProvider(resource=resource)
tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(tp)
tracer = trace.get_tracer("payments-api")

# ---- Metrics ----
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

mr = PeriodicExportingMetricReader(OTLPMetricExporter())
mp = MeterProvider(resource=resource, metric_readers=[mr])
from opentelemetry import metrics
metrics.set_meter_provider(mp)
meter = metrics.get_meter("payments-api")
latency = meter.create_histogram("http.server.request.duration", unit="s")

# ---- Logs ----
import logging
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

lp = LoggerProvider(resource=resource)
lp.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
root = logging.getLogger()
root.addHandler(LoggingHandler(logger_provider=lp))
root.setLevel(logging.INFO)

# ---- Example span + metric + log ----
import time, random
with tracer.start_as_current_span("charge") as span:
    amount = random.randint(50, 200) / 100.0
    span.set_attribute("payment.amount", amount)
    start = time.perf_counter()
    # ... call PSP ...
    dur = time.perf_counter() - start
    latency.record(dur)
    logging.info("payment processed", extra={"amount": amount})
```

This pattern maps 1:1 to the Python SDK docs for traces, metrics, and logs. ([GitHub][1])

---

## 13) Troubleshooting & testing

* **In‑memory exporters** (spans, metrics) help unit‑test your instrumentation without a backend. Use `SimpleSpanProcessor + InMemorySpanExporter`, or the spec’s in‑memory metrics exporter. ([OpenTelemetry][37])
* **Shutdown hangs / missing flush:** ensure you call `shutdown()` (or `force_flush()`) on providers before exit; this is required by the specs and avoids data loss. ([OpenTelemetry][20])
* **Performance/concurrency:** consider exporter/processor tuning for highly concurrent workloads; watch Python SDK issue tracker for concurrency notes. ([GitHub][38])

---

## 14) Reference: common configuration examples

**Minimal environment**

```bash
export OTEL_SERVICE_NAME="frontend"
export OTEL_RESOURCE_ATTRIBUTES="service.version=1.10,deployment.environment.name=prod"
export OTEL_PROPAGATORS="tracecontext,baggage"
export OTEL_TRACES_SAMPLER="parentbased_traceidratio"
export OTEL_TRACES_SAMPLER_ARG="0.1"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317"  # gRPC
```

Env var semantics are defined in the cross‑language spec and the language SDK docs. ([OpenTelemetry][39])

**Enable B3 propagation (interop)**

```bash
export OTEL_PROPAGATORS="tracecontext,baggage,b3"
```

Accepted propagator names and defaults are listed in the general SDK configuration. ([OpenTelemetry][33])

**Disable specific auto‑instrumentations**

```bash
export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS="redis,kafka-python"
```

([OpenTelemetry][30])

**Opt into stable HTTP semconv**

```bash
export OTEL_SEMCONV_STABILITY_OPT_IN="http"   # or "http/dup" during migration
```

([OpenTelemetry][35])

---

## 15) Why this guidance works

* It aligns with **OTel’s official Python docs** and the **specification** (sampling, exporters, metrics, logs) rather than third‑party conventions. ([GitHub][1])
* It uses the **Collector** as the central policy plane (tail sampling, PII redaction, routing), which the project itself positions as best practice. ([OpenTelemetry][9])
* It follows **W3C Trace Context + Baggage** as defaults and documents how to interop with B3/Jaeger when needed. ([W3C][17])

---

### Appendix A — Deep links (selected)

* **Python landing / getting started / instrumentation / exporters**: official pages. ([OpenTelemetry][2])
* **Auto‑instrumentation example & configuration** (agent, CLI flags): official pages. ([OpenTelemetry][40])
* **ASGI/FastAPI** docs: Python‑contrib. ([OpenTelemetry Python Contrib][31])
* **HTTP & DB semantic conventions** (stable): spec pages. ([OpenTelemetry][15])
* **OTLP protocol, ports, and configuration** (4317 gRPC / 4318 HTTP): spec & guidance. ([OpenTelemetry][10])
* **Kubernetes enrichment** via k8sattributes processor / Helm preset. ([OpenTelemetry][41])
* **Tail sampling & scaling considerations** in Collector. ([OpenTelemetry][12])
* **Logs: Python SDK docs & auto‑instrumentation**: status + how‑to. ([OpenTelemetry Python][27])

---

## Final checklist (use this when you wire up a new service)

1. **Set** `service.name`, `service.version`, `deployment.environment.name`. ([OpenTelemetry][14])
2. **Export via OTLP → Collector** (gRPC 4317 if allowed). ([OpenTelemetry][10])
3. **Use** `ParentBased(TraceIdRatioBased)` head sampling; add Collector tail sampling for error/latency classes. ([OpenTelemetry][33])
4. **Adopt HTTP/DB semconv** and migrate HTTP attributes using `OTEL_SEMCONV_STABILITY_OPT_IN`. ([OpenTelemetry][15])
5. **Enable log correlation** (`OTEL_PYTHON_LOG_CORRELATION=true`). ([OpenTelemetry Python Contrib][28])
6. **Harden data** with Collector attribute/redaction/transform processors. ([OpenTelemetry][13])
7. **Tune metrics** via Views (drop high‑cardinality labels; set histogram buckets). ([OpenTelemetry Python][22])
8. **Gracefully shutdown** providers (flush/export) on exit. ([OpenTelemetry][20])
9. **Pre‑fork servers:** init providers post‑fork or use fork hooks. ([OpenTelemetry Python][21])

If you adopt the patterns and defaults above, you’ll meet or exceed current best practice for OpenTelemetry in Python while staying close to the spec and leaving room for future evolution.

[1]: https://github.com/open-telemetry/opentelemetry-python?utm_source=chatgpt.com "OpenTelemetry Python API and SDK"
[2]: https://opentelemetry.io/docs/languages/python/?utm_source=chatgpt.com "Python"
[3]: https://opentelemetry-python.readthedocs.io/en/latest/exporter/otlp/otlp.html?utm_source=chatgpt.com "OpenTelemetry OTLP Exporters"
[4]: https://opentelemetry-python-contrib.readthedocs.io/?utm_source=chatgpt.com "OpenTelemetry-Python-Contrib - Read the Docs"
[5]: https://opentelemetry.io/docs/zero-code/python/?utm_source=chatgpt.com "Python zero-code instrumentation"
[6]: https://signoz.io/opentelemetry/python-auto-instrumentation/?utm_source=chatgpt.com "Auto-instrumentation of Python applications with ..."
[7]: https://opentelemetry.io/docs/specs/semconv/http/?utm_source=chatgpt.com "Semantic conventions for HTTP"
[8]: https://opentelemetry.io/ecosystem/registry/?utm_source=chatgpt.com "Registry"
[9]: https://opentelemetry.io/docs/languages/python/exporters/?utm_source=chatgpt.com "Exporters"
[10]: https://opentelemetry.io/docs/specs/otlp/?utm_source=chatgpt.com "OTLP Specification 1.8.0"
[11]: https://signoz.io/comparisons/opentelemetry-grpc-vs-http/?utm_source=chatgpt.com "OpenTelemetry - gRPC vs HTTP for Efficient Tracing"
[12]: https://opentelemetry.io/docs/concepts/sampling/?utm_source=chatgpt.com "Sampling"
[13]: https://opentelemetry.io/docs/security/handling-sensitive-data/?utm_source=chatgpt.com "Handling sensitive data"
[14]: https://opentelemetry.io/docs/concepts/resources/?utm_source=chatgpt.com "Resources"
[15]: https://opentelemetry.io/docs/specs/semconv/http/http-spans/?utm_source=chatgpt.com "Semantic conventions for HTTP spans"
[16]: https://opentelemetry.io/docs/specs/semconv/database/database-spans/?utm_source=chatgpt.com "Semantic conventions for database client spans"
[17]: https://www.w3.org/TR/trace-context/?utm_source=chatgpt.com "Trace Context"
[18]: https://opentelemetry-python.readthedocs.io/en/latest/api/propagate.html?utm_source=chatgpt.com "opentelemetry.propagate package"
[19]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.sampling.html?utm_source=chatgpt.com "opentelemetry.sdk.trace.sampling"
[20]: https://opentelemetry.io/docs/specs/otel/trace/sdk/?utm_source=chatgpt.com "Tracing SDK"
[21]: https://opentelemetry-python.readthedocs.io/en/stable/examples/fork-process-model/README.html?utm_source=chatgpt.com "Working With Fork Process Models - OpenTelemetry Python"
[22]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html?utm_source=chatgpt.com "opentelemetry.sdk.metrics.view"
[23]: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.html?utm_source=chatgpt.com "opentelemetry.sdk.metrics package"
[24]: https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/otlp/?utm_source=chatgpt.com "Metrics Exporter - OTLP"
[25]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/?utm_source=chatgpt.com "Semantic conventions for HTTP metrics"
[26]: https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanmetricsprocessor?utm_source=chatgpt.com "spanmetricsprocessor package - github.com/open- ..."
[27]: https://opentelemetry-python.readthedocs.io/en/stable/sdk/_logs.html?utm_source=chatgpt.com "opentelemetry.sdk._logs package"
[28]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html?utm_source=chatgpt.com "OpenTelemetry Logging Instrumentation"
[29]: https://docs.datadoghq.com/tracing/other_telemetry/connect_logs_and_traces/opentelemetry/?utm_source=chatgpt.com "Correlating OpenTelemetry Traces and Logs"
[30]: https://opentelemetry.io/docs/zero-code/python/configuration/?utm_source=chatgpt.com "Agent Configuration - Python"
[31]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/asgi/asgi.html?utm_source=chatgpt.com "OpenTelemetry ASGI Instrumentation"
[32]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/sqlalchemy/sqlalchemy.html?utm_source=chatgpt.com "OpenTelemetry SQLAlchemy Instrumentation"
[33]: https://opentelemetry.io/docs/languages/sdk-configuration/general/?utm_source=chatgpt.com "General SDK Configuration"
[34]: https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/?utm_source=chatgpt.com "OTLP Exporter Configuration"
[35]: https://opentelemetry.io/docs/specs/semconv/non-normative/http-migration/?utm_source=chatgpt.com "HTTP semantic convention stability migration"
[36]: https://grafana.com/docs/alloy/latest/reference/components/otelcol/otelcol.connector.spanmetrics/?utm_source=chatgpt.com "otelcol.connector.spanmetrics | Grafana Alloy documentation"
[37]: https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/in-memory/?utm_source=chatgpt.com "Metrics Exporter - In-memory"
[38]: https://github.com/open-telemetry/opentelemetry-python/issues/3663?utm_source=chatgpt.com "When utilizing the Opentelemetry SDK to export traces, ..."
[39]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/?utm_source=chatgpt.com "Environment Variable Specification"
[40]: https://opentelemetry.io/docs/zero-code/python/example/?utm_source=chatgpt.com "Auto-Instrumentation Example"
[41]: https://opentelemetry.io/docs/platforms/kubernetes/collector/components/?utm_source=chatgpt.com "Important Components for Kubernetes"
