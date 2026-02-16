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
    from obs.otel.cache import cache_span
    from obs.otel.config import OtelConfig, OtelConfigOverrides, OtelConfigSpec, resolve_otel_config
    from obs.otel.diagnostics_bundle import (
        diagnostics_bundle_enabled,
        snapshot_diagnostics,
        write_run_diagnostics_bundle,
    )
    from obs.otel.heartbeat import (
        HeartbeatController,
        set_heartbeat_blockers,
        start_build_heartbeat,
    )
    from obs.otel.logs import OtelDiagnosticsSink, emit_diagnostics_event
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
    from obs.otel.run_context import (
        clear_query_id,
        clear_run_id,
        get_query_id,
        get_run_id,
        reset_query_id,
        reset_run_id,
        set_query_id,
        set_run_id,
    )
    from obs.otel.scopes import (
        SCOPE_CPG,
        SCOPE_DATAFUSION,
        SCOPE_DIAGNOSTICS,
        SCOPE_EXECUTION,
        SCOPE_EXTRACT,
        SCOPE_HAMILTON,
        SCOPE_NORMALIZE,
        SCOPE_OBS,
        SCOPE_PIPELINE,
        SCOPE_PLANNING,
        SCOPE_ROOT,
        SCOPE_SCHEDULING,
        SCOPE_SEMANTICS,
        SCOPE_STORAGE,
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
        stage_span,
    )

__all__ = [
    "SCOPE_CPG",
    "SCOPE_DATAFUSION",
    "SCOPE_DIAGNOSTICS",
    "SCOPE_EXECUTION",
    "SCOPE_EXTRACT",
    "SCOPE_HAMILTON",
    "SCOPE_NORMALIZE",
    "SCOPE_OBS",
    "SCOPE_PIPELINE",
    "SCOPE_PLANNING",
    "SCOPE_ROOT",
    "SCOPE_SCHEDULING",
    "SCOPE_SEMANTICS",
    "SCOPE_STORAGE",
    "HeartbeatController",
    "OtelBootstrapOptions",
    "OtelConfig",
    "OtelConfigOverrides",
    "OtelConfigSpec",
    "OtelDiagnosticsSink",
    "OtelProviders",
    "cache_span",
    "clear_query_id",
    "clear_run_id",
    "configure_otel",
    "diagnostics_bundle_enabled",
    "emit_diagnostics_event",
    "get_query_id",
    "get_run_id",
    "get_tracer",
    "metric_views",
    "record_artifact_count",
    "record_datafusion_duration",
    "record_error",
    "record_exception",
    "record_stage_duration",
    "record_task_duration",
    "record_write_duration",
    "reset_query_id",
    "reset_run_id",
    "resolve_otel_config",
    "root_span",
    "root_span_context",
    "root_span_link",
    "set_dataset_stats",
    "set_heartbeat_blockers",
    "set_query_id",
    "set_root_span_context",
    "set_run_id",
    "set_scan_telemetry",
    "set_span_attributes",
    "snapshot_diagnostics",
    "span_attributes",
    "stage_span",
    "start_build_heartbeat",
    "write_run_diagnostics_bundle",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "HeartbeatController": ("obs.otel.heartbeat", "HeartbeatController"),
    "OtelConfig": ("obs.otel.config", "OtelConfig"),
    "OtelConfigOverrides": ("obs.otel.config", "OtelConfigOverrides"),
    "OtelConfigSpec": ("obs.otel.config", "OtelConfigSpec"),
    "OtelBootstrapOptions": ("obs.otel.bootstrap", "OtelBootstrapOptions"),
    "OtelProviders": ("obs.otel.bootstrap", "OtelProviders"),
    "resolve_otel_config": ("obs.otel.config", "resolve_otel_config"),
    "configure_otel": ("obs.otel.bootstrap", "configure_otel"),
    "diagnostics_bundle_enabled": (
        "obs.otel.diagnostics_bundle",
        "diagnostics_bundle_enabled",
    ),
    "OtelDiagnosticsSink": ("obs.otel.logs", "OtelDiagnosticsSink"),
    "emit_diagnostics_event": ("obs.otel.logs", "emit_diagnostics_event"),
    "get_query_id": ("obs.otel.run_context", "get_query_id"),
    "get_run_id": ("obs.otel.run_context", "get_run_id"),
    "set_query_id": ("obs.otel.run_context", "set_query_id"),
    "set_run_id": ("obs.otel.run_context", "set_run_id"),
    "reset_query_id": ("obs.otel.run_context", "reset_query_id"),
    "reset_run_id": ("obs.otel.run_context", "reset_run_id"),
    "clear_query_id": ("obs.otel.run_context", "clear_query_id"),
    "clear_run_id": ("obs.otel.run_context", "clear_run_id"),
    "cache_span": ("obs.otel.cache", "cache_span"),
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
    "stage_span": ("obs.otel.tracing", "stage_span"),
    "SCOPE_ROOT": ("obs.otel.scopes", "SCOPE_ROOT"),
    "SCOPE_PIPELINE": ("obs.otel.scopes", "SCOPE_PIPELINE"),
    "SCOPE_EXTRACT": ("obs.otel.scopes", "SCOPE_EXTRACT"),
    "SCOPE_NORMALIZE": ("obs.otel.scopes", "SCOPE_NORMALIZE"),
    "SCOPE_PLANNING": ("obs.otel.scopes", "SCOPE_PLANNING"),
    "SCOPE_SCHEDULING": ("obs.otel.scopes", "SCOPE_SCHEDULING"),
    "SCOPE_DATAFUSION": ("obs.otel.scopes", "SCOPE_DATAFUSION"),
    "SCOPE_EXECUTION": ("obs.otel.scopes", "SCOPE_EXECUTION"),
    "SCOPE_STORAGE": ("obs.otel.scopes", "SCOPE_STORAGE"),
    "SCOPE_HAMILTON": ("obs.otel.scopes", "SCOPE_HAMILTON"),
    "SCOPE_CPG": ("obs.otel.scopes", "SCOPE_CPG"),
    "SCOPE_OBS": ("obs.otel.scopes", "SCOPE_OBS"),
    "SCOPE_DIAGNOSTICS": ("obs.otel.scopes", "SCOPE_DIAGNOSTICS"),
    "SCOPE_SEMANTICS": ("obs.otel.scopes", "SCOPE_SEMANTICS"),
    "set_dataset_stats": ("obs.otel.metrics", "set_dataset_stats"),
    "set_root_span_context": ("obs.otel.tracing", "set_root_span_context"),
    "set_scan_telemetry": ("obs.otel.metrics", "set_scan_telemetry"),
    "set_span_attributes": ("obs.otel.tracing", "set_span_attributes"),
    "snapshot_diagnostics": ("obs.otel.diagnostics_bundle", "snapshot_diagnostics"),
    "span_attributes": ("obs.otel.tracing", "span_attributes"),
    "set_heartbeat_blockers": ("obs.otel.heartbeat", "set_heartbeat_blockers"),
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
