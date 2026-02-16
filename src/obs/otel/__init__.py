"""OpenTelemetry helpers for CodeAnatomy observability."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from obs.otel.bootstrap import (
        OtelBootstrapOptions,
        OtelProviders,
        configure_otel,
    )
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

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "HeartbeatController": ("obs.otel.heartbeat", "HeartbeatController"),
    "OtelBootstrapOptions": ("obs.otel.bootstrap", "OtelBootstrapOptions"),
    "OtelProviders": ("obs.otel.bootstrap", "OtelProviders"),
    "configure_otel": ("obs.otel.bootstrap", "configure_otel"),
    "diagnostics_bundle_enabled": (
        "obs.otel.diagnostics_bundle",
        "diagnostics_bundle_enabled",
    ),
    "emit_diagnostics_event": ("obs.otel.logs", "emit_diagnostics_event"),
    "get_tracer": ("obs.otel.tracing", "get_tracer"),
    "metric_views": ("obs.otel.metrics", "metric_views"),
    "record_artifact_count": ("obs.otel.metrics", "record_artifact_count"),
    "record_datafusion_duration": ("obs.otel.metrics", "record_datafusion_duration"),
    "record_error": ("obs.otel.metrics", "record_error"),
    "record_exception": ("obs.otel.tracing", "record_exception"),
    "record_stage_duration": ("obs.otel.metrics", "record_stage_duration"),
    "record_task_duration": ("obs.otel.metrics", "record_task_duration"),
    "record_write_duration": ("obs.otel.metrics", "record_write_duration"),
    "root_span": ("obs.otel.tracing", "root_span"),
    "root_span_context": ("obs.otel.tracing", "root_span_context"),
    "root_span_link": ("obs.otel.tracing", "root_span_link"),
    "set_dataset_stats": ("obs.otel.metrics", "set_dataset_stats"),
    "set_root_span_context": ("obs.otel.tracing", "set_root_span_context"),
    "set_scan_telemetry": ("obs.otel.metrics", "set_scan_telemetry"),
    "set_span_attributes": ("obs.otel.tracing", "set_span_attributes"),
    "snapshot_diagnostics": ("obs.otel.diagnostics_bundle", "snapshot_diagnostics"),
    "span_attributes": ("obs.otel.tracing", "span_attributes"),
    "start_build_heartbeat": ("obs.otel.heartbeat", "start_build_heartbeat"),
    "write_run_diagnostics_bundle": (
        "obs.otel.diagnostics_bundle",
        "write_run_diagnostics_bundle",
    ),
}


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr_name = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
