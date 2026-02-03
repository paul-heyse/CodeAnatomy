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


def push_stage(stage: str) -> Token[tuple[str, ...]]:
    """Push a stage name onto the current context stack.

    Returns
    -------
    Token[tuple[str, ...]]
        Token used to restore the previous stack.
    """
    stack = _STAGE_STACK.get()
    return _STAGE_STACK.set((*stack, stage))


def pop_stage(token: Token[tuple[str, ...]]) -> None:
    """Restore the stage stack from the provided token."""
    _STAGE_STACK.reset(token)


def current_stage() -> str | None:
    """Return the current stage name, if any.

    Returns
    -------
    str | None
        Current stage name when present.
    """
    stack = _STAGE_STACK.get()
    return stack[-1] if stack else None


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
    "start_build_heartbeat",
]
