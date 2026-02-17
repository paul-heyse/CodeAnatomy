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
    *,
    ok: bool,
    command: str | None,
    parse_ms: float,
    exec_ms: float,
    exit_code: int,
    error_class: str | None = None,
    error_stage: str | None = None,
    event_id: str | None = None,
    event_uuid_version: int | None = None,
    event_created_ms: int | None = None,
) -> CqInvokeEvent:
    """Build one CQ invocation event payload.

    Returns:
    -------
    CqInvokeEvent
        Structured invocation telemetry event.
    """
    return CqInvokeEvent(
        ok=ok,
        command=command,
        parse_ms=parse_ms,
        exec_ms=exec_ms,
        exit_code=exit_code,
        error_class=error_class,
        error_stage=error_stage,
        event_id=event_id,
        event_uuid_version=event_uuid_version,
        event_created_ms=event_created_ms,
    )


__all__ = ["CqInvokeEvent", "build_invoke_event"]
