"""Unit tests for UDF ladder enforcement."""

from __future__ import annotations

from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.udf.metadata import get_default_udf_catalog
from datafusion_engine.udf.platform import ensure_rust_udfs
from tests.test_helpers.datafusion_runtime import df_ctx
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


def test_udf_tier_tags_and_lane_precedence(require_native_runtime: None) -> None:
    """Prefer DataFusion UDF lanes in the ladder."""
    _ = require_native_runtime
    ctx = df_ctx()
    ensure_rust_udfs(ctx)
    introspector = SchemaIntrospector(ctx, sql_options=None)
    catalog = get_default_udf_catalog(introspector=introspector)
    resolved = catalog.resolve_function("stable_hash64")
    assert resolved is not None
    assert resolved.tier == "builtin"
    assert resolved.resolved_name == "stable_hash64"
