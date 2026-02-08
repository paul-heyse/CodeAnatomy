"""Tests for compiled cache policy hierarchy in the semantic pipeline.

Verifies the three-tier cache policy resolution:
1. Explicit ``cache_policy`` override (highest priority)
2. ``compiled_cache_policy`` from topology-based policy compiler
3. ``_default_semantic_cache_policy()`` naming heuristic (fallback)
"""

from __future__ import annotations

import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.views.artifacts import CachePolicy
from semantics.pipeline import (
    _default_semantic_cache_policy,
    _resolve_cache_policy_hierarchy,
)


@pytest.fixture
def sample_view_names() -> list[str]:
    """Return a representative set of view names for testing.

    Returns:
    -------
    list[str]
        View names spanning cpg, relationship, normalization, and join kinds.
    """
    return [
        "cpg_nodes",
        "cpg_edges",
        "rel_calls",
        "cst_refs_norm",
        "join_refs_defs",
        "dim_exported_defs",
    ]


@pytest.fixture
def sample_output_locations() -> dict[str, DatasetLocation]:
    """Return a minimal output locations mapping for heuristic fallback testing.

    Returns:
    -------
    dict[str, DatasetLocation]
        Mapping with ``cpg_nodes`` as a known output location.
    """
    return {"cpg_nodes": DatasetLocation(path="/tmp/cpg_nodes")}


class TestResolveCachePolicyHierarchy:
    """Test the three-tier cache policy resolution hierarchy."""

    def test_explicit_policy_takes_highest_precedence(
        self,
        sample_view_names: list[str],
        sample_output_locations: dict[str, DatasetLocation],
    ) -> None:
        """Verify explicit cache_policy overrides both compiled and heuristic."""
        explicit: dict[str, CachePolicy] = {"cpg_nodes": "none", "rel_calls": "delta_output"}
        compiled = {"cpg_nodes": "delta_staging", "rel_calls": "delta_staging"}

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=explicit,
            compiled_policy=compiled,
            view_names=sample_view_names,
            output_locations=sample_output_locations,
        )

        assert result is explicit
        assert result["cpg_nodes"] == "none"
        assert result["rel_calls"] == "delta_output"

    def test_compiled_policy_takes_precedence_over_heuristic(
        self,
        sample_view_names: list[str],
        sample_output_locations: dict[str, DatasetLocation],
    ) -> None:
        """Verify compiled_cache_policy is used when explicit policy is absent."""
        compiled = {
            "cpg_nodes": "delta_staging",
            "cpg_edges": "none",
            "rel_calls": "delta_output",
            "cst_refs_norm": "none",
            "join_refs_defs": "delta_output",
            "dim_exported_defs": "none",
        }

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=compiled,
            view_names=sample_view_names,
            output_locations=sample_output_locations,
        )

        assert result is compiled
        assert result["cpg_nodes"] == "delta_staging"
        assert result["cpg_edges"] == "none"
        assert result["rel_calls"] == "delta_output"

    def test_heuristic_fallback_when_both_policies_absent(
        self,
        sample_view_names: list[str],
        sample_output_locations: dict[str, DatasetLocation],
    ) -> None:
        """Verify naming heuristic is used when no compiled or explicit policy exists."""
        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=None,
            view_names=sample_view_names,
            output_locations=sample_output_locations,
        )

        heuristic = _default_semantic_cache_policy(
            view_names=sample_view_names,
            output_locations=sample_output_locations,
        )
        assert result == heuristic

    def test_heuristic_assigns_delta_output_for_cpg_in_locations(
        self,
        sample_output_locations: dict[str, DatasetLocation],
    ) -> None:
        """Verify the naming heuristic gives delta_output to cpg views with locations."""
        result = _default_semantic_cache_policy(
            view_names=["cpg_nodes", "cpg_edges"],
            output_locations=sample_output_locations,
        )

        assert result["cpg_nodes"] == "delta_output"
        assert result["cpg_edges"] == "delta_staging"

    def test_heuristic_assigns_delta_staging_for_rel_and_norm_views(self) -> None:
        """Verify the naming heuristic assigns delta_staging to rel_ and _norm views."""
        result = _default_semantic_cache_policy(
            view_names=["rel_calls", "cst_refs_norm"],
            output_locations={},
        )

        assert result["rel_calls"] == "delta_staging"
        assert result["cst_refs_norm"] == "delta_staging"

    def test_empty_compiled_policy_is_used_not_skipped(
        self,
        sample_view_names: list[str],
        sample_output_locations: dict[str, DatasetLocation],
    ) -> None:
        """Verify an empty compiled policy mapping is not treated as absent."""
        compiled: dict[str, str] = {}

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=compiled,
            view_names=sample_view_names,
            output_locations=sample_output_locations,
        )

        # Empty dict is truthy for identity but will produce "none" for all views
        # via _cache_policy_for lookups. The point is it is NOT the heuristic.
        assert result is compiled
