"""Structured logging helpers for OpenTelemetry."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

import msgspec
from opentelemetry import _logs, trace
from opentelemetry._logs import SeverityNumber
from opentelemetry.trace import Span
from opentelemetry.util.types import AttributeValue

from obs.otel.attributes import normalize_attributes, normalize_log_attributes
from obs.otel.scope_metadata import instrumentation_schema_url, instrumentation_version
from obs.otel.scopes import SCOPE_DIAGNOSTICS
from serde_msgspec import JSON_ENCODER

_LOGGER = logging.getLogger(SCOPE_DIAGNOSTICS)


class OtelDiagnosticsSink:
    """Diagnostics sink that emits artifacts/events to OpenTelemetry logs."""

    @staticmethod
    def record_artifact(name: str, payload: Mapping[str, object]) -> None:
        """Record an artifact payload as a diagnostics event.

        Parameters
        ----------
        name
            Artifact name.
        payload
            Artifact payload to log.
        """
        emit_diagnostics_event(name, payload=payload, event_kind="artifact")

    @staticmethod
    def record_event(name: str, properties: Mapping[str, object]) -> None:
        """Record a diagnostics event payload.

        Parameters
        ----------
        name
            Event name.
        properties
            Event properties to log.
        """
        emit_diagnostics_event(name, payload=properties, event_kind="event")

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Record multiple diagnostics events.

        Parameters
        ----------
        name
            Event name.
        rows
            Event payloads to log.
        """
        for row in rows:
            self.record_event(name, dict(row))

    @staticmethod
    def events_snapshot() -> dict[str, list[Mapping[str, object]]]:
        """Return collected event payloads (not stored for OTel sinks).

        Returns
        -------
        dict[str, list[Mapping[str, object]]]
            Empty mapping because OpenTelemetry sinks stream events directly.
        """
        return {}

    @staticmethod
    def artifacts_snapshot() -> dict[str, list[Mapping[str, object]]]:
        """Return collected artifact payloads (not stored for OTel sinks).

        Returns
        -------
        dict[str, list[Mapping[str, object]]]
            Empty mapping because OpenTelemetry sinks stream artifacts directly.
        """
        return {}


def _serialize_payload(payload: object) -> str:
    try:
        return JSON_ENCODER.encode(payload).decode("utf-8")
    except (msgspec.EncodeError, TypeError, ValueError):
        return str(payload)


def _flatten_attributes(payload: Mapping[str, object]) -> dict[str, object]:
    flattened: dict[str, object] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, Mapping):
            flattened[key] = _serialize_payload(value)
            continue
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            flattened[key] = _serialize_payload(list(value))
            continue
        flattened[key] = value
    return flattened


def emit_diagnostics_event(
    name: str,
    *,
    payload: Mapping[str, object],
    level: int = logging.INFO,
    event_kind: str | None = None,
) -> None:
    """Emit a diagnostics event as a structured log record.

    Parameters
    ----------
    name
        Event name.
    payload
        Event payload attributes.
    level
        Logging level to emit.
    event_kind
        Optional event kind label.
    """
    attributes: dict[str, object] = {"event.name": name}
    if event_kind is not None:
        attributes["event.kind"] = event_kind
    attributes.update(_flatten_attributes(payload))
    normalized = normalize_log_attributes(attributes)
    if _otel_logging_enabled():
        _emit_otel_log(name=name, attributes=normalized, level=level)
    else:
        _LOGGER.log(level, name, extra=normalized)
    span = trace.get_current_span()
    _emit_span_event(span, name=name, event_kind=event_kind, payload=payload)


def _emit_span_event(
    span: Span,
    *,
    name: str,
    event_kind: str | None,
    payload: Mapping[str, object],
) -> None:
    if not span.is_recording():
        return
    event_attrs: dict[str, object] = {"event.kind": event_kind} if event_kind else {}
    event_attrs.update(_flatten_attributes(payload))
    span.add_event(name, attributes=normalize_attributes(event_attrs))


def _otel_logging_enabled() -> bool:
    provider = _logs.get_logger_provider()
    return provider.__class__.__name__ != "ProxyLoggerProvider"


def _emit_otel_log(*, name: str, attributes: Mapping[str, AttributeValue], level: int) -> None:
    version_value = instrumentation_version()
    version = version_value if version_value is not None else "unknown"
    logger = _logs.get_logger(
        SCOPE_DIAGNOSTICS,
        version,
        schema_url=instrumentation_schema_url(),
    )
    severity_number = _severity_from_level(level)
    logger.emit(
        severity_number=severity_number,
        severity_text=logging.getLevelName(level),
        body=name,
        attributes=attributes,
        event_name=name,
    )


def _severity_from_level(level: int) -> SeverityNumber:
    if level >= logging.CRITICAL:
        return SeverityNumber.FATAL
    if level >= logging.ERROR:
        return SeverityNumber.ERROR
    if level >= logging.WARNING:
        return SeverityNumber.WARN
    if level >= logging.INFO:
        return SeverityNumber.INFO
    return SeverityNumber.DEBUG


__all__ = ["OtelDiagnosticsSink", "emit_diagnostics_event"]
