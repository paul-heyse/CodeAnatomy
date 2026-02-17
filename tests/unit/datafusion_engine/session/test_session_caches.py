"""Tests for shared runtime session caches."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.session._session_caches import (
    RUNTIME_SETTINGS_OVERLAY,
    SESSION_CONTEXT_CACHE,
    SESSION_RUNTIME_CACHE,
)


def test_session_caches_are_mutable_singletons() -> None:
    """Session cache maps hold mutable singleton entries per key."""
    key = "test-session-key"
    marker = SessionContext()
    SESSION_CONTEXT_CACHE[key] = marker
    SESSION_RUNTIME_CACHE[key] = marker
    assert SESSION_CONTEXT_CACHE[key] is marker
    assert SESSION_RUNTIME_CACHE[key] is marker
    del SESSION_CONTEXT_CACHE[key]
    del SESSION_RUNTIME_CACHE[key]


def test_runtime_settings_overlay_is_available() -> None:
    """Runtime settings overlay singleton is available."""
    assert RUNTIME_SETTINGS_OVERLAY is not None
