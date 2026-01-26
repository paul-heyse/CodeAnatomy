"""Unit tests for UDF ladder enforcement."""

from __future__ import annotations

from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.udf_catalog import datafusion_udf_specs
from datafusion_engine.udf_runtime import register_rust_udfs
from engine.function_registry import FunctionRegistryOptions, build_function_registry


def test_udf_tier_tags_and_lane_precedence() -> None:
    """Prefer DataFusion UDF lanes in the ladder."""
    profile = DataFusionRuntimeProfile()
    session = profile.session_context()
    registry_snapshot = register_rust_udfs(session)
    options = FunctionRegistryOptions(
        datafusion_specs=datafusion_udf_specs(registry_snapshot=registry_snapshot),
        ibis_specs=(),
        primitives=(),
    )
    registry = build_function_registry(options=options)
    spec = registry.specs["stable_hash64"]
    assert spec.udf_tier == "builtin"
    assert "ibis_builtin" not in spec.lanes
    assert registry.resolve_lane("stable_hash64") == "df_rust"
