"""Unit tests for UDF ladder enforcement."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.udf_catalog import get_default_udf_catalog
from datafusion_engine.udf_runtime import register_rust_udfs


def test_udf_tier_tags_and_lane_precedence() -> None:
    """Prefer DataFusion UDF lanes in the ladder."""
    ctx = SessionContext()
    register_rust_udfs(ctx)
    introspector = SchemaIntrospector(ctx, sql_options=None)
    catalog = get_default_udf_catalog(introspector=introspector)
    resolved = catalog.resolve_function("stable_hash64")
    assert resolved is not None
    assert resolved.tier == "builtin"
    assert resolved.resolved_name == "stable_hash64"
