"""Runtime-capability event normalization and summary helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeExecutionMetricsSummary:
    """Normalized runtime execution metrics summary fields."""

    rows: int | None = None
    memory_reserved_bytes: int | None = None
    metadata_cache_entries: int | None = None
    metadata_cache_hits: int | None = None
    list_files_cache_entries: int | None = None
    statistics_cache_entries: int | None = None


@dataclass(frozen=True)
class RuntimeCapabilityEvent:
    """Normalized runtime capability event payload."""

    event_time_unix_ms: int
    strict_native_provider_enabled: object
    delta_available: object
    delta_compatible: object
    delta_probe_result: object
    delta_ctx_kind: object
    delta_module: object
    plugin_manifest: Mapping[str, object] | None
    execution_metrics: Mapping[str, object] | None


def collect_runtime_capability_events(
    logs: Sequence[Mapping[str, object]],
) -> tuple[RuntimeCapabilityEvent, ...]:
    """Collect normalized runtime capability events with source-priority fallback.

    Returns:
    -------
    tuple[RuntimeCapabilityEvent, ...]
        Normalized runtime capability events.
    """
    runtime_events = _runtime_capability_events(logs)
    if runtime_events:
        return runtime_events
    extension_events = _extension_parity_events(logs)
    if extension_events:
        return extension_events
    return _service_provider_events(logs)


def latest_runtime_capability_event(
    events: Sequence[RuntimeCapabilityEvent],
) -> RuntimeCapabilityEvent | None:
    """Return the latest runtime capability event.

    Returns:
    -------
    RuntimeCapabilityEvent | None
        Latest event by ``event_time_unix_ms``, or ``None`` when no events exist.
    """
    if not events:
        return None
    return max(events, key=lambda event: event.event_time_unix_ms)


def runtime_capability_summary_payload(
    events: Sequence[RuntimeCapabilityEvent],
) -> dict[str, object]:
    """Build the stable runtime capability summary payload.

    Returns:
    -------
    dict[str, object]
        Stable runtime-capability summary mapping.
    """
    latest = latest_runtime_capability_event(events)
    if latest is None:
        return {"total": 0}
    metrics_summary = summarize_runtime_execution_metrics(latest.execution_metrics)
    return {
        "total": len(events),
        "strict_native_provider_enabled": latest.strict_native_provider_enabled,
        "delta_available": latest.delta_available,
        "delta_compatible": latest.delta_compatible,
        "delta_probe_result": latest.delta_probe_result,
        "delta_ctx_kind": latest.delta_ctx_kind,
        "delta_module": latest.delta_module,
        "plugin_path": _plugin_path(latest.plugin_manifest),
        "execution_metrics_rows": metrics_summary.rows,
        "execution_memory_reserved_bytes": metrics_summary.memory_reserved_bytes,
        "execution_metadata_cache_entries": metrics_summary.metadata_cache_entries,
        "execution_metadata_cache_hits": metrics_summary.metadata_cache_hits,
        "execution_list_files_cache_entries": metrics_summary.list_files_cache_entries,
        "execution_statistics_cache_entries": metrics_summary.statistics_cache_entries,
    }


def summarize_runtime_execution_metrics(
    payload: Mapping[str, object] | None,
) -> RuntimeExecutionMetricsSummary:
    """Normalize runtime execution metrics payload into stable summary fields.

    Returns:
    -------
    RuntimeExecutionMetricsSummary
        Normalized execution metrics fields used by diagnostics summaries.
    """
    if payload is None:
        return RuntimeExecutionMetricsSummary()
    rows = payload.get("rows")
    row_count = (
        len(rows)
        if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray))
        else None
    )
    summary = payload.get("summary")
    if not isinstance(summary, Mapping):
        return RuntimeExecutionMetricsSummary(rows=row_count)
    return RuntimeExecutionMetricsSummary(
        rows=row_count,
        memory_reserved_bytes=_coerce_number(summary.get("memory_reserved_bytes")),
        metadata_cache_entries=_coerce_number(summary.get("metadata_cache_entries")),
        metadata_cache_hits=_coerce_number(summary.get("metadata_cache_hits")),
        list_files_cache_entries=_coerce_number(summary.get("list_files_cache_entries")),
        statistics_cache_entries=_coerce_number(summary.get("statistics_cache_entries")),
    )


def _event_rows(
    logs: Sequence[Mapping[str, object]],
    name: str,
) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    for record in logs:
        attrs = record.get("attributes")
        if not isinstance(attrs, Mapping):
            continue
        if attrs.get("event.name") != name:
            continue
        rows.append(attrs)
    return rows


def _runtime_capability_events(
    logs: Sequence[Mapping[str, object]],
) -> tuple[RuntimeCapabilityEvent, ...]:
    rows = _event_rows(logs, "datafusion_runtime_capabilities_v1")
    return tuple(_event_from_runtime_row(row) for row in rows)


def _extension_parity_events(
    logs: Sequence[Mapping[str, object]],
) -> tuple[RuntimeCapabilityEvent, ...]:
    rows = _event_rows(logs, "datafusion_extension_parity_v1")
    events: list[RuntimeCapabilityEvent] = []
    for row in rows:
        payload = row.get("runtime_capabilities")
        if not isinstance(payload, Mapping):
            continue
        events.append(_event_from_runtime_row(payload))
    return tuple(events)


def _service_provider_events(
    logs: Sequence[Mapping[str, object]],
) -> tuple[RuntimeCapabilityEvent, ...]:
    rows = _event_rows(logs, "delta_service_provider_v1")
    return tuple(_event_from_service_row(row) for row in rows)


def _event_from_runtime_row(row: Mapping[str, object]) -> RuntimeCapabilityEvent:
    plugin_manifest = row.get("plugin_manifest")
    execution_metrics = row.get("execution_metrics")
    return RuntimeCapabilityEvent(
        event_time_unix_ms=_event_time_unix_ms(row),
        strict_native_provider_enabled=row.get("strict_native_provider_enabled"),
        delta_available=row.get("delta_available"),
        delta_compatible=row.get("delta_compatible"),
        delta_probe_result=row.get("delta_probe_result"),
        delta_ctx_kind=row.get("delta_ctx_kind"),
        delta_module=row.get("delta_module"),
        plugin_manifest=_mapping_or_none(plugin_manifest),
        execution_metrics=_mapping_or_none(execution_metrics),
    )


def _event_from_service_row(row: Mapping[str, object]) -> RuntimeCapabilityEvent:
    return RuntimeCapabilityEvent(
        event_time_unix_ms=_event_time_unix_ms(row),
        strict_native_provider_enabled=row.get("strict_native_provider_enabled"),
        delta_available=row.get("available"),
        delta_compatible=row.get("compatible"),
        delta_probe_result=row.get("probe_result"),
        delta_ctx_kind=row.get("ctx_kind"),
        delta_module=row.get("module"),
        plugin_manifest=None,
        execution_metrics=None,
    )


def _event_time_unix_ms(row: Mapping[str, object]) -> int:
    value = row.get("event_time_unix_ms")
    if isinstance(value, int):
        return value
    return 0


def _mapping_or_none(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    return None


def _plugin_path(plugin_manifest: Mapping[str, object] | None) -> str | None:
    if plugin_manifest is None:
        return None
    path = plugin_manifest.get("plugin_path")
    if isinstance(path, str):
        return path
    return None


def _coerce_number(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


__all__ = [
    "RuntimeCapabilityEvent",
    "RuntimeExecutionMetricsSummary",
    "collect_runtime_capability_events",
    "latest_runtime_capability_event",
    "runtime_capability_summary_payload",
    "summarize_runtime_execution_metrics",
]
