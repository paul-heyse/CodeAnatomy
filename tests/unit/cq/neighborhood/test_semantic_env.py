"""Tests for neighborhood semantic environment extraction."""

from __future__ import annotations

from tools.cq.core.snb_schema import BundleMetaV1, SemanticNeighborhoodBundleV1
from tools.cq.neighborhood.semantic_env import semantic_env_from_bundle


def test_semantic_env_from_bundle_returns_empty_for_non_bundle() -> None:
    """Non-bundle values should produce an empty semantic environment."""
    assert semantic_env_from_bundle(object()) == {}


def test_semantic_env_from_bundle_extracts_known_flags() -> None:
    """Known semantic metadata keys should map into normalized env fields."""
    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="bundle-1",
        meta=BundleMetaV1(
            semantic_sources=(
                {
                    "workspace_health": "ok",
                    "quiescent": True,
                    "position_encoding": "utf-16",
                },
            )
        ),
    )
    assert semantic_env_from_bundle(bundle) == {
        "semantic_health": "ok",
        "semantic_quiescent": True,
        "semantic_position_encoding": "utf-16",
    }
