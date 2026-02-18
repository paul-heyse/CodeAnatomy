"""Tests for profile-scoped runtime session caches."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_session_caches_are_scoped_to_runtime_profile() -> None:
    """Session context/runtime caches are isolated per runtime profile."""
    profile_a = DataFusionRuntimeProfile()
    profile_b = DataFusionRuntimeProfile()

    marker = SessionContext()
    profile_a.session_context_cache["test"] = marker
    profile_a.session_runtime_cache["test"] = object()

    assert profile_a.session_context_cache["test"] is marker
    assert "test" in profile_a.session_runtime_cache
    assert "test" not in profile_b.session_context_cache
    assert "test" not in profile_b.session_runtime_cache


def test_runtime_settings_overlay_is_profile_scoped() -> None:
    """Runtime settings overlays are isolated per runtime profile."""
    profile_a = DataFusionRuntimeProfile()
    profile_b = DataFusionRuntimeProfile()
    ctx = SessionContext()

    profile_a.runtime_settings_overlay[ctx] = {"datafusion.runtime.list_files_cache_ttl": "60"}

    assert profile_a.runtime_settings_overlay.get(ctx) == {
        "datafusion.runtime.list_files_cache_ttl": "60"
    }
    assert profile_b.runtime_settings_overlay.get(ctx) is None
