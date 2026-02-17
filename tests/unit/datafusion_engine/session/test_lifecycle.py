"""Tests for session lifecycle helper functions."""

from __future__ import annotations

from datafusion_engine.session.lifecycle import context_cache_key, create_session_context
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_lifecycle_helpers_use_runtime_profile() -> None:
    """Lifecycle helpers create and key session context from profile."""
    profile = DataFusionRuntimeProfile()

    ctx = create_session_context(profile)

    assert ctx is profile.session_context()
    assert context_cache_key(profile)
