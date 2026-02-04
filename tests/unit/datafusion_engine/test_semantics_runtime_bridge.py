"""Tests for semantic runtime bridge helpers."""

from __future__ import annotations

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.semantics_runtime import (
    apply_semantic_runtime_config,
    semantic_runtime_from_profile,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, DataSourceConfig


def test_semantic_runtime_round_trip() -> None:
    """Semantic runtime config should round-trip through the bridge."""
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            semantic_output_locations={
                "cpg_nodes_v1": DatasetLocation(path="/tmp/semantic_nodes", format="delta")
            }
        ),
    )
    runtime_config = semantic_runtime_from_profile(profile)
    assert runtime_config.output_locations["cpg_nodes_v1"] == "/tmp/semantic_nodes"

    updated = apply_semantic_runtime_config(profile, runtime_config)
    location = updated.data_sources.semantic_output_locations.get("cpg_nodes_v1")
    assert location is not None
    assert str(location.path) == "/tmp/semantic_nodes"
