"""Tests for runtime profile convenience helpers."""

from __future__ import annotations

from datafusion_engine.session.profiles import runtime_for_profile
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_runtime_for_profile_returns_session_runtime() -> None:
    """runtime_for_profile returns runtime bound to the input profile."""
    profile = DataFusionRuntimeProfile()

    runtime = runtime_for_profile(profile)

    assert runtime.profile is profile
