"""Integration tests for semantic runtime config in engine sessions."""

from __future__ import annotations

import pytest

from engine.runtime_profile import resolve_runtime_profile
from engine.session_factory import EngineSessionOptions, build_engine_session
from semantics.runtime import SemanticRuntimeConfig


@pytest.mark.integration
def test_engine_session_applies_semantic_runtime_config() -> None:
    """Engine session should apply semantic output locations and cache overrides."""
    runtime_spec = resolve_runtime_profile("default")
    semantic_config = SemanticRuntimeConfig(
        output_locations={"cpg_nodes_v1": "/tmp/semantic_cpg_nodes"},
        cache_policy_overrides={"cpg_nodes_v1": "delta_output"},
    )
    session = build_engine_session(
        runtime_spec=runtime_spec,
        options=EngineSessionOptions(semantic_config=semantic_config),
    )
    profile = session.datafusion_profile
    location = profile.dataset_location("cpg_nodes_v1")
    assert location is not None
    assert str(location.path) == "/tmp/semantic_cpg_nodes"
    assert profile.data_sources.semantic_cache_overrides["cpg_nodes_v1"] == "delta_output"
