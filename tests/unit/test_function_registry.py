"""Tests for function registry merging and lane selection."""

from __future__ import annotations

from engine.function_registry import default_function_registry


def test_function_registry_merges_udf_lanes() -> None:
    """Merge function lanes from Ibis and DataFusion sources."""
    registry = default_function_registry()
    spec = registry.specs["stable_hash64"]
    assert {
        "ibis_builtin",
        "ibis_pyarrow",
        "ibis_python",
        "df_udf",
        "df_rust",
    }.issubset(set(spec.lanes))


def test_function_registry_resolve_lane_prefers_ibis() -> None:
    """Resolve the preferred lane using registry precedence."""
    registry = default_function_registry()
    assert registry.resolve_lane("stable_hash64") == "ibis_builtin"
    assert registry.resolve_lane("normalize_span") == "df_udf"
