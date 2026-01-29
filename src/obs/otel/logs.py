"""Structured logging helpers for OpenTelemetry."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence

from opentelemetry import trace
from opentelemetry.trace import Span

from obs.otel.attributes import normalize_attributes
from obs.otel.scopes import SCOPE_DIAGNOSTICS

_LOGGER = logging.getLogger(SCOPE_DIAGNOSTICS)


def _serialize_payload(payload: object) -> str:
    try:
        return json.dumps(payload, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return str(payload)


def _flatten_attributes(payload: Mapping[str, object]) -> dict[str, object]:
    flattened: dict[str, object] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, Mapping):
            flattened[key] = _serialize_payload(value)
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
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
    span = trace.get_current_span()
    context = span.get_span_context()
    if context.is_valid:
        attributes["trace_id"] = f"{context.trace_id:032x}"
        attributes["span_id"] = f"{context.span_id:016x}"
    attributes.update(_flatten_attributes(payload))
    normalized = normalize_attributes(attributes)
    _LOGGER.log(level, name, extra=normalized)
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


__all__ = ["emit_diagnostics_event"]
