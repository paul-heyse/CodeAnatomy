"""Tests for CLI telemetry event builders."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.telemetry_events import CqInvokeEvent, build_invoke_event

_PARSE_MS = 1.25
_EXEC_MS = 5.5
_EVENT_UUID_VERSION = 7
_EVENT_CREATED_MS = 123456


def test_build_invoke_event_populates_fields() -> None:
    """Invoke event builder preserves provided telemetry values."""
    event = build_invoke_event(
        CqInvokeEvent(
            ok=True,
            command="search",
            parse_ms=_PARSE_MS,
            exec_ms=_EXEC_MS,
            exit_code=0,
            event_id="run_1",
            event_uuid_version=_EVENT_UUID_VERSION,
            event_created_ms=_EVENT_CREATED_MS,
        )
    )

    assert isinstance(event, CqInvokeEvent)
    assert event.ok is True
    assert event.command == "search"
    assert event.parse_ms == pytest.approx(_PARSE_MS)
    assert event.exec_ms == pytest.approx(_EXEC_MS)
    assert event.exit_code == 0
    assert event.event_id == "run_1"
    assert event.event_uuid_version == _EVENT_UUID_VERSION
    assert event.event_created_ms == _EVENT_CREATED_MS


def test_build_invoke_event_with_error_fields() -> None:
    """Invoke event builder carries optional error metadata."""
    event = build_invoke_event(
        CqInvokeEvent(
            ok=False,
            command="search",
            parse_ms=0.1,
            exec_ms=0.2,
            exit_code=2,
            error_class="cyclopts.UnknownCommandError",
            error_stage="command_resolve",
        )
    )

    assert event.ok is False
    assert event.error_class == "cyclopts.UnknownCommandError"
    assert event.error_stage == "command_resolve"
