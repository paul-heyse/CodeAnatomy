# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from datafusion_engine.session.profiles import runtime_for_profile
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_runtime_for_profile_returns_session_runtime() -> None:
    profile = DataFusionRuntimeProfile()

    runtime = runtime_for_profile(profile, use_cache=False)

    assert runtime.profile is profile
