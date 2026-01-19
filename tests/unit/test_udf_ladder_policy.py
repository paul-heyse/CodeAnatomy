"""Unit tests for UDF ladder enforcement."""

from __future__ import annotations

from datafusion_engine.udf_registry import datafusion_udf_specs
from engine.function_registry import build_function_registry


def test_udf_tier_tags_and_lane_precedence() -> None:
    """Prefer DataFusion UDF lanes in the ladder."""
    registry = build_function_registry(
        datafusion_specs=datafusion_udf_specs(),
        ibis_specs=(),
        primitives=(),
    )
    spec = registry.specs["stable_hash64"]
    assert spec.udf_tier == "python"
    assert "ibis_builtin" not in spec.lanes
    assert registry.resolve_lane("stable_hash64") == "df_udf"
