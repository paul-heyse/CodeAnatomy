"""Tests for extraction engine-session runtime behavior."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.dataset.registry import DatasetCatalog
from datafusion_engine.session.profiles import create_runtime_profile
from extraction.engine_runtime import EngineRuntime
from extraction.engine_session import EngineSession


def test_engine_session_prefers_runtime_session_context_override() -> None:
    """EngineSession should prefer explicitly provided runtime session context."""
    profile = create_runtime_profile()
    override_ctx = SessionContext()
    runtime = EngineRuntime(datafusion_profile=profile, session_context=override_ctx)
    session = EngineSession(engine_runtime=runtime, datasets=DatasetCatalog())

    assert session.df_ctx() is override_ctx
    assert session.datafusion_facade().ctx is override_ctx
