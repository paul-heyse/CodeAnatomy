"""Shared telemetry event contracts for CQ CLI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CqInvokeEvent:
    """Structured CQ invocation telemetry payload."""

    ok: bool
    command: str | None
    parse_ms: float
    exec_ms: float
    exit_code: int
    error_class: str | None = None
    error_stage: str | None = None
    event_id: str | None = None
    event_uuid_version: int | None = None
    event_created_ms: int | None = None


def build_invoke_event(
    event: CqInvokeEvent,
) -> CqInvokeEvent:
    """Build one CQ invocation event payload.

    Returns:
    -------
    CqInvokeEvent
        Structured invocation telemetry event.
    """
    return event


__all__ = ["CqInvokeEvent", "build_invoke_event"]
