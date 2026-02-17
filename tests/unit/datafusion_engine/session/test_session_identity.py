# ruff: noqa: D103
"""Tests for session identity authority constants."""

from __future__ import annotations

from datafusion_engine.session._session_identity import RUNTIME_SESSION_ID


def test_runtime_session_id_is_process_stable() -> None:
    from datafusion_engine.session._session_identity import (
        RUNTIME_SESSION_ID as SECOND_RUNTIME_SESSION_ID,
    )

    assert isinstance(RUNTIME_SESSION_ID, str)
    assert RUNTIME_SESSION_ID
    assert SECOND_RUNTIME_SESSION_ID == RUNTIME_SESSION_ID
