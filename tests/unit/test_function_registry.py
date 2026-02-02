"""Tests for DataFusion UDF catalog behavior."""

from __future__ import annotations

from datafusion_engine.schema.introspection import SchemaIntrospector
from datafusion_engine.udf.catalog import get_default_udf_catalog
from datafusion_engine.udf.platform import ensure_rust_udfs
from tests.test_helpers.datafusion_runtime import df_ctx
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


def _udf_catalog() -> SchemaIntrospector:
    ctx = df_ctx()
    ensure_rust_udfs(ctx)
    return SchemaIntrospector(ctx, sql_options=None)


def test_udf_catalog_resolves_builtin() -> None:
    """Resolve Rust builtin UDFs from the catalog."""
    introspector = _udf_catalog()
    catalog = get_default_udf_catalog(introspector=introspector)
    resolved = catalog.resolve_function("stable_hash64")
    assert resolved is not None
    assert resolved.tier == "builtin"
    assert resolved.resolved_name == "stable_hash64"
