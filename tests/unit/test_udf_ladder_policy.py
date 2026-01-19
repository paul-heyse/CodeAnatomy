"""Unit tests for UDF ladder enforcement."""

from __future__ import annotations

from engine.function_registry import build_function_registry
from ibis_engine.builtin_udfs import ibis_udf_specs


def test_udf_tier_tags_and_lane_precedence() -> None:
    """Prefer builtin lanes in the UDF ladder."""
    registry = build_function_registry(
        datafusion_specs=(),
        ibis_specs=ibis_udf_specs(),
        primitives=(),
    )
    spec = registry.specs["stable_hash64"]
    assert spec.udf_tier == "builtin"
    assert registry.resolve_lane("stable_hash64") == "ibis_builtin"
