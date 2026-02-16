"""Tests for CQ schema export helpers."""

from __future__ import annotations

from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema

MIN_SCHEMA_DOCS = 2


def test_cq_result_schema_exports() -> None:
    """Test cq result schema exports."""
    schema = cq_result_schema()
    assert isinstance(schema, dict)
    assert "$ref" in schema
    defs = schema.get("$defs")
    assert isinstance(defs, dict)
    assert "CqResult" in defs


def test_query_schema_exports() -> None:
    """Test query schema exports."""
    schema = query_schema()
    assert isinstance(schema, dict)
    assert "$ref" in schema
    defs = schema.get("$defs")
    assert isinstance(defs, dict)
    assert "Query" in defs


def test_cq_schema_components_exports() -> None:
    """Test cq schema components exports."""
    schema_docs, components = cq_schema_components()
    assert isinstance(schema_docs, tuple)
    assert len(schema_docs) >= MIN_SCHEMA_DOCS
    assert isinstance(components, dict)
    assert components
    component_keys = set(components)
    assert "RunPlan" in component_keys or "RunStep" in component_keys
    assert "TreeSitterArtifactBundleV1" in component_keys
