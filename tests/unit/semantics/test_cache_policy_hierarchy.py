"""Tests for explicit/compiled cache policy resolution in semantic pipeline."""

from __future__ import annotations

from datafusion_engine.views.artifacts import CachePolicy
from semantics.pipeline import _resolve_cache_policy_hierarchy


class TestResolveCachePolicyHierarchy:
    """Test explicit -> compiled cache policy layering."""

    @staticmethod
    def test_explicit_policy_takes_highest_precedence() -> None:
        """Explicit cache policy overrides compiled mappings."""
        explicit: dict[str, CachePolicy] = {"cpg_nodes": "none", "rel_calls": "delta_output"}
        compiled: dict[str, CachePolicy] = {
            "cpg_nodes": "delta_staging",
            "rel_calls": "delta_staging",
        }

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=explicit,
            compiled_policy=compiled,
        )

        assert result is explicit
        assert result["cpg_nodes"] == "none"
        assert result["rel_calls"] == "delta_output"

    @staticmethod
    def test_compiled_policy_takes_precedence_when_explicit_absent() -> None:
        """Compiled cache policy should be used when explicit policy is absent."""
        compiled: dict[str, CachePolicy] = {
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
        )

        assert result is compiled
        assert result["cpg_nodes"] == "delta_staging"
        assert result["cpg_edges"] == "none"
        assert result["rel_calls"] == "delta_output"

    @staticmethod
    def test_returns_empty_mapping_when_explicit_and_compiled_absent() -> None:
        """Missing explicit/compiled cache policy should resolve to empty mapping."""
        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=None,
        )

        assert result == {}

    @staticmethod
    def test_empty_compiled_policy_is_used_not_skipped() -> None:
        """An empty compiled mapping is still treated as authoritative."""
        compiled: dict[str, CachePolicy] = {}

        result = _resolve_cache_policy_hierarchy(
            explicit_policy=None,
            compiled_policy=compiled,
        )

        assert result is compiled
