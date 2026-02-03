"""OpenTelemetry helpers for CodeAnatomy observability."""

from __future__ import annotations

from obs.otel.bootstrap import OtelBootstrapOptions, OtelProviders, configure_otel
from obs.otel.diagnostics_bundle import (
    diagnostics_bundle_enabled,
    snapshot_diagnostics,
    write_run_diagnostics_bundle,
)
from obs.otel.heartbeat import HeartbeatController, start_build_heartbeat
from obs.otel.logs import emit_diagnostics_event
from obs.otel.metrics import (
    metric_views,
    record_artifact_count,
    record_datafusion_duration,
    record_error,
    record_stage_duration,
    record_task_duration,
    record_write_duration,
    set_dataset_stats,
    set_scan_telemetry,
)
from obs.otel.tracing import (
    get_tracer,
    record_exception,
    root_span,
    root_span_context,
    root_span_link,
    set_root_span_context,
    set_span_attributes,
    span_attributes,
)

__all__ = [
    "HeartbeatController",
    "OtelBootstrapOptions",
    "OtelProviders",
    "configure_otel",
    "diagnostics_bundle_enabled",
    "emit_diagnostics_event",
    "get_tracer",
    "metric_views",
    "record_artifact_count",
    "record_datafusion_duration",
    "record_error",
    "record_exception",
    "record_stage_duration",
    "record_task_duration",
    "record_write_duration",
    "root_span",
    "root_span_context",
    "root_span_link",
    "set_dataset_stats",
    "set_root_span_context",
    "set_scan_telemetry",
    "set_span_attributes",
    "snapshot_diagnostics",
    "span_attributes",
    "start_build_heartbeat",
    "write_run_diagnostics_bundle",
]
