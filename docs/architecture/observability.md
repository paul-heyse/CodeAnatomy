# Observability Architecture (OpenTelemetry)

## Overview
CodeAnatomy emits best-in-class observability data using OpenTelemetry (OTel) for traces, metrics, and logs. The observability design is split into two layers:

1. **Domain semantics (`src/obs/`)**: domain-specific signals (diagnostics, scan telemetry, dataset stats).
2. **Transport (`src/obs/otel/`)**: OTel SDK wiring, exporters, resource identity, and correlation.

This separation allows the codebase to evolve domain signals without coupling them to a specific telemetry backend.

**Key Modules:**
- `bootstrap.py`: Provider configuration and initialization
- `config.py`: Environment-based configuration resolution
- `metrics.py`: Metric instruments and recording functions
- `tracing.py`: Span creation and lifecycle helpers
- `logs.py`: Structured logging via OTel logs API
- `hamilton.py`: Hamilton lifecycle hooks for automatic instrumentation
- `processors.py`: Custom span and log processors (`RunIdSpanProcessor`, `RunIdLogRecordProcessor`)
- `resources.py`: Resource construction helpers
- `resource_detectors.py`: Configurable resource detector integration
- `run_context.py`: Run-scoped context variables
- `sampling.py`: Sampling rule wrappers
- `attributes.py`: Attribute normalization helpers
- `constants.py`: Canonical metric, attribute, and scope names
- `scopes.py`: Instrumentation scope definitions

---

## Bootstrap and Provider Configuration
The OTel bootstrap layer is implemented in `src/obs/otel/bootstrap.py`. All entry points initialize OTel through this layer.

**Primary Function:**
```python
def configure_otel(
    *,
    service_name: str | None = None,
    options: OtelBootstrapOptions | None = None,
) -> OtelProviders
```

Key responsibilities:
- Build the **Resource** (service identity + environment attributes via `resources.py`).
- Configure **TracerProvider**, **MeterProvider**, and **LoggerProvider**.
- Add custom processors: `RunIdSpanProcessor` and `RunIdLogRecordProcessor`.
- Export via **OTLP** (gRPC or HTTP/protobuf) to an OTel Collector by default.
- Enable log correlation through the OTel logging instrumentation.
- Support test mode with in-memory exporters for contract validation.

The bootstrap reads standard `OTEL_*` env vars and respects `OTEL_SDK_DISABLED`.

**Configuration Resolution:**
Configuration is resolved via `src/obs/otel/config.py`:
- `resolve_otel_config()`: Parses environment variables into `OtelConfig` dataclass
- Supports sampler entrypoints via `opentelemetry_traces_sampler`
- Resolves batch processor settings with queue size validation
- Handles exemplar filters, temporality preferences, and histogram aggregations

---

## Resource Identity
Resource attributes are stable and process-level:
- `service.name`: Resolved from `OTEL_SERVICE_NAME` or defaults to `codeanatomy`
- `service.version`: Resolved from `CODEANATOMY_SERVICE_VERSION` or package metadata
- `service.namespace`: Resolved from `CODEANATOMY_SERVICE_NAMESPACE`
- `service.instance.id`: Resolved from `OTEL_SERVICE_INSTANCE_ID`, `CODEANATOMY_SERVICE_INSTANCE_ID`, or secure random token
- `deployment.environment.name`: Resolved from `CODEANATOMY_ENVIRONMENT` or `DEPLOYMENT_ENVIRONMENT`

Run-specific details (plan signature, run id, dataset names) are captured at the **span** or **log** level instead of the Resource.

**Resource Detection:**
Resource detector enrichment is controlled via `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` (e.g., `process,os,host,container,k8s`). When unset, the default detector set is `process,os,host,container,k8s` with missing detectors ignored and logged.

Resource construction flow:
1. `build_resource()` creates base resource with service identity
2. `build_detected_resource()` applies configured resource detectors
3. `merge_resource_overrides()` applies final overrides from `OtelBootstrapOptions.resource_overrides`

---

## Trace Topology
A single pipeline run emits a root span plus child spans for stages and tasks.

**Root span**
- Name: `graph_product.build` or custom via `root_span()` context manager
- Scope: `codeanatomy.pipeline` (default)
- Attributes: product, execution mode, outputs, run_id
- Context stored via `set_root_span_context()` for span linking

**Pipeline span**
- Name: `pipeline.execute`
- Scope: `codeanatomy.pipeline`
- Attributes: execution mode, outputs, repo_root

**Hamilton spans** (via `src/obs/otel/hamilton.py`)
- **Graph span**: `hamilton.graph` - emitted by `OtelPlanHook`
- **Node spans**: `hamilton.task`, `hamilton.stage`, `hamilton.node` - emitted by `OtelNodeHook`
- Attributes: `run_id`, `node_name`, `task_id`, `codeanatomy.run_id`, `hamilton.execution_run_id`, plus all Hamilton tags prefixed with `hamilton.`
- Scopes: Determined by `scope_for_layer()` mapping layer tags to canonical scopes

**Stage spans** (via `stage_span()` context manager)
- Emit both span and `codeanatomy.stage.duration` metric
- Automatic exception recording and status tracking

**DataFusion spans**
- `datafusion.plan.compile`
- `datafusion.execute`
- `datafusion.write`
- `datafusion.write_view`

Span links are used to connect task spans back to the root span when the execution graph is asynchronous. Links created via `root_span_link()` helper.

---

## Metrics Catalog
Metrics are emitted via `src/obs/otel/metrics.py`:

**Histograms**
- `codeanatomy.stage.duration`
- `codeanatomy.task.duration`
- `codeanatomy.datafusion.execute.duration`
- `codeanatomy.datafusion.write.duration`

**Counters**
- `codeanatomy.artifact.count`
- `codeanatomy.error.count`

**Gauges**
- `codeanatomy.dataset.rows`
- `codeanatomy.dataset.columns`
- `codeanatomy.scan.row_groups`
- `codeanatomy.scan.fragments`

Cardinality controls are applied via Views to keep labels bounded.

---

## Logs and Diagnostics
Diagnostics events and artifacts are emitted as OTel logs via `DiagnosticsCollector` and `emit_diagnostics_event()`:

**Emission Flow:**
1. `DiagnosticsCollector.record_event()` or `record_artifact()` called with name and payload
2. Calls `emit_diagnostics_event()` from `src/obs/otel/logs.py`
3. Payload is flattened (nested structures serialized to JSON strings)
4. Emitted as both:
   - OTel log record via `Logger.emit()` (when OTel logging is enabled)
   - Span event on current span (when span is recording)
5. Increments `codeanatomy.artifact.count` metric

**Structured Attributes:**
- `event.name`: Event identifier
- `event.kind`: `"event"` or `"artifact"`
- Flattened payload fields

**Log Correlation:**
- Enabled via `OTEL_PYTHON_LOG_CORRELATION=true` (default)
- Implemented via `opentelemetry.instrumentation.logging.LoggingInstrumentor`
- Adds trace_id, span_id, trace_flags to log records
- LoggingHandler installed on root logger unless auto-instrumentation logging is enabled

This allows diagnostics artifacts to be queryable and correlated to traces and spans.

---

## Collector Configuration (Cache Telemetry)
Cache telemetry is designed to flow through an OTel Collector to enforce **tail sampling** and **redaction** before export. The reference pipeline below:
- Drops high-cardinality cache path attributes.
- Applies redaction to common path prefixes.
- Generates span-derived metrics for cache latency/error analysis.

**Reference Collector YAML:**
```yaml
processors:
  attributes:
    actions:
      - key: cache.path
        action: delete
  redaction:
    blocked_values: ["/home/", "s3://"]
  tail_sampling:
    policies:
      - name: cache-errors
        type: status_code
        status_code: {status_codes: [ERROR]}
      - name: cache-latency
        type: latency
        latency: {threshold_ms: 2000}
connectors:
  spanmetrics: {}

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [attributes, redaction, tail_sampling]
      exporters: [spanmetrics, otlp]
    metrics:
      receivers: [spanmetrics]
      exporters: [otlp]
```

**Redaction Guidance:**
- Remove or hash file paths and repo roots at the Collector layer.
- Keep `cache.policy`, `cache.scope`, and `cache.operation` as bounded dimensions.
- Prefer Delta ledger tables for forensic details that are too high-cardinality for OTel.

## Auto-Instrumentation Boundaries
CodeAnatomy uses auto-instrumentation for commodity libraries (HTTP clients, databases, gRPC) and manual instrumentation for pipeline stages and DataFusion internals.

Recommended environment controls:
- `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS` to disable noisy instrumentations.
- `OTEL_PYTHON_EXCLUDED_URLS` to drop internal endpoints.

Manual instrumentation should always set attributes at span creation to allow sampler policies to evaluate them.

---

## Custom Processors
**File:** `src/obs/otel/processors.py`

CodeAnatomy adds custom processors to inject run-scoped context:

**RunIdSpanProcessor:**
- Implements `SpanProcessor` protocol
- Attaches `codeanatomy.run_id` attribute to all spans on `on_start()`
- Retrieves run_id from `get_run_id()` context variable

**RunIdLogRecordProcessor:**
- Implements `LogRecordProcessor` protocol
- Attaches `codeanatomy.run_id` attribute to all log records on `on_emit()`
- Retrieves run_id from `get_run_id()` context variable

These processors ensure consistent run correlation across all telemetry signals without manual attribute injection.

---

## Hamilton UI Telemetry
Hamilton UI run tracking is enabled via the Hamilton tracker adapter. CodeAnatomy enforces capture governance through telemetry profiles (dev/ci/prod) and SDK constants:

- **prod**: tracker enabled, statistics capture disabled, small list/dict limits.
- **ci**: tracker disabled by default, minimal capture.
- **dev**: tracker enabled, statistics capture enabled, larger list/dict limits.

Profiles can be overridden via environment variables or config.

Correlation fields:
- Hamilton tracker tags include `codeanatomy.run_id`, `otel.trace_id`, and `otel.span_id`.
- OTel spans include `codeanatomy.run_id` and `hamilton.execution_run_id` (set by `OtelNodeHook` and `OtelPlanHook`).

---

## Run Context Management
**File:** `src/obs/otel/run_context.py`

Run-scoped context is managed via `ContextVar`:

```python
def set_run_id(run_id: str) -> Token[str | None]
def get_run_id() -> str | None
def reset_run_id(token: Token[str | None]) -> None
def clear_run_id() -> None
```

This context is used by:
- Custom processors to inject `codeanatomy.run_id` into spans and logs
- Metric recording functions to add run_id tags
- Hamilton hooks to correlate pipeline and Hamilton execution runs

---

## Concurrency and Fork Safety
Extraction uses process-based parallelism. OTel is initialized inside worker processes to avoid fork-safety issues with batch processors. Threaded execution propagates context explicitly where needed.

Root span context is stored in both `ContextVar` and a fallback dict to support context propagation across process boundaries.

---

## Collector-First Deployment
All signals export via OTLP to a Collector. The Collector is responsible for:
- Tail sampling
- Attribute redaction
- Routing to downstream backends

Reference configuration: `tools/otel/collector.yaml`.

---

## Environment Variables (Selected)

### Service Identity
- `OTEL_SERVICE_NAME`: Service name (default: `codeanatomy`)
- `CODEANATOMY_SERVICE_VERSION`: Service version (falls back to package version)
- `CODEANATOMY_SERVICE_NAMESPACE`: Service namespace
- `OTEL_SERVICE_INSTANCE_ID`, `CODEANATOMY_SERVICE_INSTANCE_ID`: Instance ID (falls back to secure random token)
- `CODEANATOMY_ENVIRONMENT`, `DEPLOYMENT_ENVIRONMENT`: Deployment environment name
- `OTEL_RESOURCE_ATTRIBUTES`: Additional resource attributes (comma-separated key=value pairs)

### OTel SDK Control
- `OTEL_SDK_DISABLED`: Disable all OTel instrumentation (default: false)
- `OTEL_EXPERIMENTAL_CONFIG_FILE`: Use declarative configuration file
- `OTEL_PYTHON_CONTEXT`: Python context implementation
- `OTEL_PYTHON_ID_GENERATOR`: ID generator (default, random, xray)

### Sampling
- `OTEL_TRACES_SAMPLER`: Sampler name (default: `parentbased_traceidratio`)
- `OTEL_TRACES_SAMPLER_ARG`: Sampler argument (ratio for traceidratio, default: 0.1)
- `CODEANATOMY_OTEL_SAMPLING_RULE`: Sampling rule identifier (default: `codeanatomy.default`)

### Exporters
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint for all signals
- `OTEL_EXPORTER_OTLP_PROTOCOL`: Protocol (grpc, http/protobuf, default: grpc)
- `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`, `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`, `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL`: Signal-specific protocols
- `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER`: Exporter type (set to `none` to disable signal)

### Resource Detectors
- `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`: Comma-separated detector names (default: `process,os,host,container,k8s`)

### Batch Processors
- `OTEL_BSP_SCHEDULE_DELAY`, `OTEL_BLRP_SCHEDULE_DELAY`: Schedule delay in ms (default: 5000)
- `OTEL_BSP_EXPORT_TIMEOUT`, `OTEL_BLRP_EXPORT_TIMEOUT`: Export timeout in ms (default: 30000)
- `OTEL_BSP_MAX_QUEUE_SIZE`, `OTEL_BLRP_MAX_QUEUE_SIZE`: Max queue size (default: 2048)
- `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`, `OTEL_BLRP_MAX_EXPORT_BATCH_SIZE`: Max batch size (default: 512)

### Metrics
- `OTEL_METRIC_EXPORT_INTERVAL`: Export interval in ms (default: 60000)
- `OTEL_METRIC_EXPORT_TIMEOUT`: Export timeout in ms (default: 30000)
- `OTEL_METRICS_EXEMPLAR_FILTER`: Exemplar filter (always_on, always_off, trace_based)
- `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`: Temporality (DELTA, CUMULATIVE, lowmemory)
- `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION`: Histogram aggregation (explicit_bucket_histogram, exponential_bucket_histogram)

### Logs
- `OTEL_PYTHON_LOG_CORRELATION`: Enable log correlation (default: true)
- `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED`: Use auto-instrumentation for logging (default: false)
- `OTEL_LOG_LEVEL`: OTel SDK log level (CRITICAL, ERROR, WARNING, INFO, DEBUG)

### Attribute Limits
- `OTEL_ATTRIBUTE_COUNT_LIMIT`: Global attribute count limit
- `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT`: Global attribute value length limit
- `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT`, `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT`: Span-specific limits
- `OTEL_SPAN_EVENT_COUNT_LIMIT`, `OTEL_SPAN_LINK_COUNT_LIMIT`: Span event/link limits
- `OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT`, `OTEL_LINK_ATTRIBUTE_COUNT_LIMIT`: Event/link attribute limits
- `OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT`, `OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT`: Log record limits

### Auto-Instrumentation
- `CODEANATOMY_OTEL_AUTO_INSTRUMENTATION`: Enable auto-instrumentation (default: false)
- `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS`: Comma-separated list of instrumentations to disable
- `OTEL_PYTHON_EXCLUDED_URLS`: URL patterns to exclude from auto-instrumentation
- `OTEL_SEMCONV_STABILITY_OPT_IN`: Semantic convention stability opt-in (e.g., `http,db`)

### CodeAnatomy-Specific
- `CODEANATOMY_OTEL_TEST_MODE`: Use in-memory exporters for testing (default: false)
- `CODEANATOMY_OTEL_SYSTEM_METRICS`: Enable system metrics instrumentation (default: false)

### Hamilton Telemetry
- `CODEANATOMY_HAMILTON_TELEMETRY_PROFILE`, `HAMILTON_TELEMETRY_PROFILE`: Telemetry profile (dev, ci, prod)
- `CODEANATOMY_HAMILTON_TRACKER_ENABLED`, `HAMILTON_TRACKER_ENABLED`: Enable Hamilton UI tracker
- `CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS`, `HAMILTON_CAPTURE_DATA_STATISTICS`: Capture data statistics
- `CODEANATOMY_HAMILTON_MAX_LIST_LENGTH_CAPTURE`, `HAMILTON_MAX_LIST_LENGTH_CAPTURE`: Max list length to capture
- `CODEANATOMY_HAMILTON_MAX_DICT_LENGTH_CAPTURE`, `HAMILTON_MAX_DICT_LENGTH_CAPTURE`: Max dict length to capture

---

## Operational Profiles
Recommended profiles:

### Development
```bash
export OTEL_TRACES_SAMPLER=always_on
export OTEL_METRIC_EXPORT_INTERVAL=10000
export OTEL_BSP_SCHEDULE_DELAY=1000
export CODEANATOMY_OTEL_SYSTEM_METRICS=true
```
- Always-on sampling for full trace visibility
- Faster export intervals for immediate feedback
- System metrics enabled for resource monitoring

### CI/Testing
```bash
export CODEANATOMY_OTEL_TEST_MODE=true
export OTEL_SDK_DISABLED=false
```
- In-memory exporters for contract tests
- Validates instrumentation without external dependencies
- Exporters accessible via `InMemorySpanExporter`, `InMemoryMetricReader`, `InMemoryLogRecordExporter`

### Production
```bash
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.01
export OTEL_METRIC_EXPORT_INTERVAL=60000
export OTEL_BSP_MAX_QUEUE_SIZE=4096
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=1024
export OTEL_EXPERIMENTAL_RESOURCE_DETECTORS=process,os,host,container,k8s
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```
- Parent-based 1% sampling at SDK (tail sampling at Collector)
- Standard export intervals and batch sizes
- Full resource detection
- OTLP/gRPC for efficient transport

---

## Architecture Summary

**Two-Layer Design:**

1. **Domain Layer** (`src/obs/`): Business-level observability
   - DiagnosticsCollector: Events and artifacts
   - Metrics: Dataset/column stats, quality tables
   - Scan Telemetry: Fragment-level performance
   - DataFusion Runs: Correlation IDs and lifecycle

2. **Transport Layer** (`src/obs/otel/`): OpenTelemetry integration
   - Bootstrap: Provider configuration
   - Processors: Run context injection
   - Hamilton Hooks: Automatic instrumentation
   - Helpers: Metrics, tracing, logging, resource detection

**Signal Flow:**
```
Domain Event → emit_diagnostics_event() → OTel Log Record + Span Event
Dataset Stats → set_dataset_stats() → Observable Gauge Update
Hamilton Node → OtelNodeHook → Span + Duration Metric
Stage Execution → stage_span() → Span + Stage Duration Metric
```

**Cross-Layer Integration:**
- DiagnosticsCollector calls OTel logging and metrics APIs
- Statistics helpers update OTel gauges
- Hamilton hooks emit OTel spans and metrics
- Custom processors inject run context into all signals

See `docs/architecture/part_vii_observability.md` for detailed module-level documentation on domain observability components.
