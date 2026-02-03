"""Run-scoped diagnostics bundle helpers for OpenTelemetry exports."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from obs.otel.metrics import metrics_snapshot
from utils.env_utils import env_bool

if TYPE_CHECKING:
    from collections.abc import Mapping

    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider


@dataclass
class DiagnosticsExporters:
    """References to in-memory exporters used for diagnostics snapshots."""

    span_exporter: InMemorySpanExporter | None
    log_exporter: InMemoryLogRecordExporter | None
    metric_reader: InMemoryMetricReader | None


_EXPORTERS: dict[str, DiagnosticsExporters | None] = {"value": None}


def diagnostics_bundle_enabled() -> bool:
    """Return whether diagnostics bundle capture is enabled.

    Returns
    -------
    bool
        True when diagnostics bundle capture is enabled.
    """
    return env_bool("CODEANATOMY_DIAGNOSTICS_BUNDLE", default=False)


def register_span_exporter(exporter: InMemorySpanExporter | None) -> None:
    """Register an in-memory span exporter for diagnostics snapshots."""
    current = _EXPORTERS["value"]
    if current is None:
        _EXPORTERS["value"] = DiagnosticsExporters(
            span_exporter=exporter,
            log_exporter=None,
            metric_reader=None,
        )
        return
    _EXPORTERS["value"] = DiagnosticsExporters(
        span_exporter=exporter,
        log_exporter=current.log_exporter,
        metric_reader=current.metric_reader,
    )


def register_log_exporter(exporter: InMemoryLogRecordExporter | None) -> None:
    """Register an in-memory log exporter for diagnostics snapshots."""
    current = _EXPORTERS["value"]
    if current is None:
        _EXPORTERS["value"] = DiagnosticsExporters(
            span_exporter=None,
            log_exporter=exporter,
            metric_reader=None,
        )
        return
    _EXPORTERS["value"] = DiagnosticsExporters(
        span_exporter=current.span_exporter,
        log_exporter=exporter,
        metric_reader=current.metric_reader,
    )


def register_metric_reader(reader: InMemoryMetricReader | None) -> None:
    """Register an in-memory metric reader for diagnostics snapshots."""
    current = _EXPORTERS["value"]
    if current is None:
        _EXPORTERS["value"] = DiagnosticsExporters(
            span_exporter=None,
            log_exporter=None,
            metric_reader=reader,
        )
        return
    _EXPORTERS["value"] = DiagnosticsExporters(
        span_exporter=current.span_exporter,
        log_exporter=current.log_exporter,
        metric_reader=reader,
    )


def _span_payload(span: ReadableSpan) -> dict[str, object]:
    start_ns = span.start_time
    end_ns = span.end_time
    duration_s = max(0.0, (end_ns - start_ns) / 1_000_000_000) if end_ns and start_ns else 0.0
    context = getattr(span, "context", None)
    trace_id = getattr(context, "trace_id", None)
    span_id = getattr(context, "span_id", None)
    return {
        "name": span.name,
        "trace_id": f"{trace_id:032x}" if isinstance(trace_id, int) else None,
        "span_id": f"{span_id:016x}" if isinstance(span_id, int) else None,
        "start_time_unix_nano": start_ns,
        "end_time_unix_nano": end_ns,
        "duration_s": duration_s,
        "attributes": dict(span.attributes or {}),
        "status": str(span.status.status_code) if span.status is not None else None,
        "events": [
            {
                "name": event.name,
                "timestamp_unix_nano": event.timestamp,
                "attributes": dict(event.attributes or {}),
            }
            for event in span.events
        ],
    }


def _log_payload(record: ReadableLogRecord) -> dict[str, object]:
    log_record = getattr(record, "log_record", record)
    trace_id = getattr(log_record, "trace_id", None)
    span_id = getattr(log_record, "span_id", None)
    severity_number = getattr(log_record, "severity_number", None)
    severity_value = int(severity_number) if isinstance(severity_number, int) else None
    return {
        "timestamp_unix_nano": getattr(log_record, "timestamp", None),
        "observed_timestamp_unix_nano": getattr(log_record, "observed_timestamp", None),
        "severity_text": getattr(log_record, "severity_text", None),
        "severity_number": severity_value,
        "body": getattr(log_record, "body", None),
        "attributes": dict(getattr(log_record, "attributes", None) or {}),
        "trace_id": f"{trace_id:032x}" if isinstance(trace_id, int) else None,
        "span_id": f"{span_id:016x}" if isinstance(span_id, int) else None,
    }


def _metric_payload(reader: InMemoryMetricReader | None) -> Mapping[str, object]:
    """Convert metric reader output into a mapping payload.

    Returns
    -------
    Mapping[str, object]
        Parsed metrics payload (empty if no reader is available).
    """
    if reader is None:
        return {}
    metrics_data = reader.get_metrics_data()
    if metrics_data is None:
        return {}
    raw = getattr(metrics_data, "to_json", None)
    if callable(raw):
        raw_text = raw()
        if isinstance(raw_text, (str, bytes, bytearray)):
            return cast("Mapping[str, object]", json.loads(raw_text))
    return {}


def _bundle_metadata(
    *,
    run_bundle_dir: Path,
    run_id: str | None,
    captured_at_unix_s: float,
) -> dict[str, object]:
    output_dir: str | None = None
    parent = run_bundle_dir.parent
    if parent.name == "run_bundle":
        output_dir = str(parent.parent)
    return {
        "run_id": run_id,
        "output_dir": output_dir,
        "captured_at_unix_s": captured_at_unix_s,
    }


def snapshot_diagnostics() -> dict[str, object]:
    """Return a snapshot of spans, logs, and metrics for diagnostics.

    Returns
    -------
    dict[str, object]
        Snapshot payload for diagnostics.
    """
    exporters = _EXPORTERS["value"]
    spans: list[dict[str, object]] = []
    logs: list[dict[str, object]] = []
    metrics: Mapping[str, object] = {}
    if exporters is not None:
        if exporters.span_exporter is not None:
            spans = [_span_payload(span) for span in exporters.span_exporter.get_finished_spans()]
        if exporters.log_exporter is not None:
            logs = [_log_payload(record) for record in exporters.log_exporter.get_finished_logs()]
        metrics = _metric_payload(exporters.metric_reader)
    return {
        "captured_at_unix_s": time.time(),
        "spans": spans,
        "logs": logs,
        "metrics": metrics,
        "gauges": metrics_snapshot(),
    }


def write_run_diagnostics_bundle(
    *,
    run_bundle_dir: Path,
    run_id: str | None,
) -> Path | None:
    """Write OpenTelemetry diagnostics bundle to the run bundle directory.

    Returns
    -------
    Path | None
        Run bundle directory when written, otherwise None.
    """
    if _EXPORTERS["value"] is None:
        return None
    payload = snapshot_diagnostics()
    captured_at = payload.get("captured_at_unix_s")
    captured_at_unix_s = (
        float(captured_at) if isinstance(captured_at, (int, float)) else time.time()
    )
    metadata = _bundle_metadata(
        run_bundle_dir=run_bundle_dir,
        run_id=run_id,
        captured_at_unix_s=captured_at_unix_s,
    )
    run_bundle_dir.mkdir(parents=True, exist_ok=True)
    trace_path = run_bundle_dir / "otel_traces.json"
    metric_path = run_bundle_dir / "otel_metrics.json"
    log_path = run_bundle_dir / "otel_logs.json"
    with trace_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "metadata": metadata,
                "spans": payload.get("spans", []),
            },
            handle,
            indent=2,
            sort_keys=True,
        )
    with metric_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "metadata": metadata,
                "metrics": payload.get("metrics", {}),
                "gauges": payload.get("gauges", {}),
            },
            handle,
            indent=2,
            sort_keys=True,
        )
    with log_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "metadata": metadata,
                "logs": payload.get("logs", []),
            },
            handle,
            indent=2,
            sort_keys=True,
        )
    return run_bundle_dir


def configure_diagnostics_exporters(
    *,
    tracer_provider: TracerProvider | None,
    meter_provider: MeterProvider | None,
    logger_provider: LoggerProvider | None,
) -> None:
    """Attach in-memory exporters to the active OpenTelemetry providers."""
    if not diagnostics_bundle_enabled():
        return
    current = _EXPORTERS["value"]
    if current is None:
        current = DiagnosticsExporters(
            span_exporter=None,
            log_exporter=None,
            metric_reader=None,
        )
        _EXPORTERS["value"] = current
    if tracer_provider is not None and current.span_exporter is None:
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        exporter = InMemorySpanExporter()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        register_span_exporter(exporter)
    if meter_provider is not None and current.metric_reader is None:
        reader = InMemoryMetricReader()
        add_reader = getattr(meter_provider, "add_metric_reader", None)
        if callable(add_reader):
            add_reader(reader)
        register_metric_reader(reader)
    if logger_provider is not None and current.log_exporter is None:
        from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor

        exporter = InMemoryLogRecordExporter()
        logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
        register_log_exporter(exporter)


__all__ = [
    "configure_diagnostics_exporters",
    "diagnostics_bundle_enabled",
    "snapshot_diagnostics",
    "write_run_diagnostics_bundle",
]
