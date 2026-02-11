"""Integration tests for semantic runtime config in engine sessions."""

from __future__ import annotations

import msgspec
import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from planning_engine.runtime_profile import resolve_runtime_profile
from planning_engine.session_factory import build_engine_session
from semantics.compile_context import build_semantic_execution_context


@pytest.mark.integration
def test_engine_session_uses_profile_semantic_output_config() -> None:
    """Engine session should use semantic output settings from the runtime profile."""
    runtime_spec = resolve_runtime_profile("default")
    datafusion_profile = msgspec.structs.replace(
        runtime_spec.datafusion,
        data_sources=msgspec.structs.replace(
            runtime_spec.datafusion.data_sources,
            semantic_output=msgspec.structs.replace(
                runtime_spec.datafusion.data_sources.semantic_output,
                locations={
                    "cpg_nodes": DatasetLocation(
                        path="/tmp/semantic_cpg_nodes",
                        format="delta",
                    )
                },
                cache_overrides={"cpg_nodes": "delta_output"},
            ),
        ),
    )
    runtime_spec = msgspec.structs.replace(
        runtime_spec,
        datafusion=datafusion_profile,
    )
    session = build_engine_session(runtime_spec=runtime_spec)
    profile = session.datafusion_profile
    location = profile.dataset_location(
        "cpg_nodes",
        dataset_resolver=build_semantic_execution_context(runtime_profile=profile).dataset_resolver,
    )
    assert location is not None
    assert str(location.path) == "/tmp/semantic_cpg_nodes"
    assert profile.data_sources.semantic_output.cache_overrides["cpg_nodes"] == "delta_output"
