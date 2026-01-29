# Observability Architecture (OpenTelemetry)

## Overview
CodeAnatomy emits best-in-class observability data using OpenTelemetry (OTel) for traces, metrics, and logs. The observability design is split into two layers:

1. **Domain semantics (`src/obs`)**: domain-specific signals (diagnostics, scan telemetry, dataset stats).
2. **Transport (`src/obs/otel`)**: OTel SDK wiring, exporters, resource identity, and correlation.

This separation allows the codebase to evolve domain signals without coupling them to a specific telemetry backend.

---

## Bootstrap and Provider Configuration
The OTel bootstrap layer is implemented in `src/obs/otel/bootstrap.py`. All entry points initialize OTel through this layer.

Key responsibilities:
- Build the **Resource** (service identity + environment attributes).
- Configure **TracerProvider**, **MeterProvider**, and **LoggerProvider**.
- Export via **OTLP** to an OTel Collector by default.
- Enable log correlation through the OTel logging pipeline.

The bootstrap reads standard `OTEL_*` env vars and respects `OTEL_SDK_DISABLED`.

---

## Resource Identity
Resource attributes are stable and process-level:
- `service.name`, `service.version`, `service.namespace`
- `deployment.environment.name`
- `codeanatomy.repo_root` (entrypoint-level override)

Run-specific details (plan signature, run id, dataset names) are captured at the **span** or **log** level instead of the Resource.

Resource detector enrichment is controlled via `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` (e.g., `process,os,host,container,k8s`). When unset, the default detector set is `process,os,host,container,k8s` with missing detectors ignored and logged.

---

## Trace Topology
A single pipeline run emits a root span plus child spans for stages and tasks.

**Root span**
- `graph_product.build`
- Attributes: product, execution mode, outputs, run_id

**Pipeline span**
- `pipeline.execute`
- Attributes: execution mode, outputs, repo_root

**Hamilton spans**
- `hamilton.task`, `hamilton.stage`, `hamilton.node`
- Attributes derived from Hamilton tags and scheduling metadata

**DataFusion spans**
- `datafusion.plan.compile`
- `datafusion.execute`
- `datafusion.write`
- `datafusion.write_view`

Span links are used to connect task spans back to the root span when the execution graph is asynchronous.

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
Diagnostics events and artifacts are emitted as OTel logs via `DiagnosticsCollector`:

- Structured log attributes include `event.name`, `event.kind`, and domain-specific payload fields.
- OTel log correlation links logs with trace/span context.

This allows diagnostics artifacts to be queryable and correlated to traces.

---

## Auto-Instrumentation Boundaries
CodeAnatomy uses auto-instrumentation for commodity libraries (HTTP clients, databases, gRPC) and manual instrumentation for pipeline stages and DataFusion internals.

Recommended environment controls:
- `OTEL_PYTHON_DISABLED_INSTRUMENTATIONS` to disable noisy instrumentations.
- `OTEL_PYTHON_EXCLUDED_URLS` to drop internal endpoints.

Manual instrumentation should always set attributes at span creation to allow sampler policies to evaluate them.

---

## Hamilton UI Telemetry
Hamilton UI run tracking is enabled via the Hamilton tracker adapter. CodeAnatomy enforces capture governance through telemetry profiles (dev/ci/prod) and SDK constants:

- **prod**: tracker enabled, statistics capture disabled, small list/dict limits.
- **ci**: tracker disabled by default, minimal capture.
- **dev**: tracker enabled, statistics capture enabled, larger list/dict limits.

Profiles can be overridden via environment variables or config.

Correlation fields:
- Hamilton tracker tags include `codeanatomy.run_id`, `otel.trace_id`, and `otel.span_id`.
- OTel spans include `codeanatomy.run_id` and `hamilton.execution_run_id`.

---

## Concurrency and Fork Safety
Extraction uses process-based parallelism. OTel is initialized inside worker processes to avoid fork-safety issues with batch processors. Threaded execution propagates context explicitly where needed.

---

## Collector-First Deployment
All signals export via OTLP to a Collector. The Collector is responsible for:
- Tail sampling
- Attribute redaction
- Routing to downstream backends

Reference configuration: `tools/otel/collector.yaml`.

---

## Environment Variables (Selected)
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`
- `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`
- `OTEL_PYTHON_LOG_CORRELATION`
- `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED`
- `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_METRIC_EXPORT_TIMEOUT`
- `OTEL_LOG_LEVEL`
- `OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT`, `OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT`
- `OTEL_METRICS_EXEMPLAR_FILTER`
- `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE`
- `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION`
- `CODEANATOMY_HAMILTON_TELEMETRY_PROFILE`, `HAMILTON_TELEMETRY_PROFILE`
- `CODEANATOMY_HAMILTON_TRACKER_ENABLED`, `HAMILTON_TRACKER_ENABLED`
- `CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS`, `HAMILTON_CAPTURE_DATA_STATISTICS`
- `CODEANATOMY_HAMILTON_MAX_LIST_LENGTH_CAPTURE`, `HAMILTON_MAX_LIST_LENGTH_CAPTURE`
- `CODEANATOMY_HAMILTON_MAX_DICT_LENGTH_CAPTURE`, `HAMILTON_MAX_DICT_LENGTH_CAPTURE`

---

## Operational Profiles
Recommended profiles:

- **Dev**: Console exporter, always-on sampling.
- **CI**: In-memory exporters for contract tests.
- **Prod**: Parent-based ratio sampling + Collector tail sampling.
