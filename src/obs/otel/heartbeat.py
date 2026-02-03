"""Build heartbeat helpers for long-running diagnostics."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass

from opentelemetry import trace

from obs.otel.attributes import normalize_attributes
from obs.otel.logs import emit_diagnostics_event
from obs.otel.metrics import metrics_snapshot
from obs.otel.scope_metadata import instrumentation_schema_url, instrumentation_version
from obs.otel.scopes import SCOPE_OBS

_STAGE_STACK: ContextVar[tuple[str, ...]] = ContextVar("codeanatomy.stage_stack", default=())
_STAGE_FALLBACK: dict[str, str | None] = {"value": None}
_STAGE_FALLBACK_LOCK = threading.Lock()
_HEARTBEAT_CONTEXT: dict[str, int | None] = {
    "active_task_count": None,
    "bytes_read": None,
    "bytes_written": None,
}
_HEARTBEAT_LOCK = threading.Lock()


def _set_fallback_stage(stage: str | None) -> None:
    with _STAGE_FALLBACK_LOCK:
        _STAGE_FALLBACK["value"] = stage


def _fallback_stage() -> str | None:
    with _STAGE_FALLBACK_LOCK:
        return _STAGE_FALLBACK["value"]


def push_stage(stage: str) -> Token[tuple[str, ...]]:
    """Push a stage name onto the current context stack.

    Returns
    -------
    Token[tuple[str, ...]]
        Token used to restore the previous stack.
    """
    stack = _STAGE_STACK.get()
    token = _STAGE_STACK.set((*stack, stage))
    _set_fallback_stage(stage)
    return token


def pop_stage(token: Token[tuple[str, ...]]) -> None:
    """Restore the stage stack from the provided token."""
    _STAGE_STACK.reset(token)
    stack = _STAGE_STACK.get()
    _set_fallback_stage(stack[-1] if stack else None)


def current_stage() -> str | None:
    """Return the current stage name, if any.

    Returns
    -------
    str | None
        Current stage name when present.
    """
    stack = _STAGE_STACK.get()
    return stack[-1] if stack else _fallback_stage()


def set_active_task_count(value: int | None) -> None:
    """Set the current active task count for heartbeat payloads."""
    with _HEARTBEAT_LOCK:
        _HEARTBEAT_CONTEXT["active_task_count"] = value


def set_io_counters(*, bytes_read: int | None = None, bytes_written: int | None = None) -> None:
    """Set IO counter hints for heartbeat payloads."""
    with _HEARTBEAT_LOCK:
        if bytes_read is not None:
            _HEARTBEAT_CONTEXT["bytes_read"] = bytes_read
        if bytes_written is not None:
            _HEARTBEAT_CONTEXT["bytes_written"] = bytes_written


def _heartbeat_snapshot() -> dict[str, int]:
    with _HEARTBEAT_LOCK:
        snapshot = dict(_HEARTBEAT_CONTEXT)
    return {key: value for key, value in snapshot.items() if value is not None}


@dataclass
class HeartbeatController:
    """Control a background heartbeat emitter."""

    stop_event: threading.Event
    thread: threading.Thread

    def stop(self, *, timeout_s: float | None = None) -> None:
        """Stop the heartbeat loop."""
        self.stop_event.set()
        self.thread.join(timeout=timeout_s)


def start_build_heartbeat(
    *,
    run_id: str,
    interval_s: float = 5.0,
    extra_payload: Callable[[], Mapping[str, object]] | None = None,
) -> HeartbeatController:
    """Start a background heartbeat emitter for the build.

    Returns
    -------
    HeartbeatController
        Controller used to stop the heartbeat.
    """
    stop_event = threading.Event()
    version = instrumentation_version() or "unknown"
    tracer = trace.get_tracer(
        SCOPE_OBS,
        version,
        schema_url=instrumentation_schema_url(),
    )

    def _emit() -> None:
        while not stop_event.wait(interval_s):
            stage = current_stage() or "unknown"
            payload: dict[str, object] = {
                "run_id": run_id,
                "stage": stage,
                "timestamp_unix_s": time.time(),
            }
            payload.update(metrics_snapshot())
            payload.update(_heartbeat_snapshot())
            if extra_payload is not None:
                payload.update(extra_payload())
            with tracer.start_as_current_span(
                "build.heartbeat",
                attributes=normalize_attributes(payload),
            ):
                emit_diagnostics_event(
                    "build_heartbeat_v1",
                    payload=payload,
                    event_kind="event",
                )

    thread = threading.Thread(target=_emit, name="build-heartbeat", daemon=True)
    thread.start()
    return HeartbeatController(stop_event=stop_event, thread=thread)


__all__ = [
    "HeartbeatController",
    "current_stage",
    "pop_stage",
    "push_stage",
    "set_active_task_count",
    "set_io_counters",
    "start_build_heartbeat",
]
