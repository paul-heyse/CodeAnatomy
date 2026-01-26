"""Tests for DataFusion UDF catalog behavior."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.udf_catalog import get_default_udf_catalog
from datafusion_engine.udf_runtime import register_rust_udfs


def _udf_catalog() -> SchemaIntrospector:
    ctx = SessionContext()
    register_rust_udfs(ctx)
    return SchemaIntrospector(ctx, sql_options=None)


def test_udf_catalog_resolves_builtin() -> None:
    """Resolve Rust builtin UDFs from the catalog."""
    introspector = _udf_catalog()
    catalog = get_default_udf_catalog(introspector=introspector)
    resolved = catalog.resolve_function("stable_hash64")
    assert resolved is not None
    assert resolved.tier == "builtin"
    assert resolved.resolved_name == "stable_hash64"
