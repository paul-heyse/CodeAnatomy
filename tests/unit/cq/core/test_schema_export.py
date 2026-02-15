"""Tests for CQ schema export helpers."""

from __future__ import annotations

from tools.cq.core.schema_export import cq_result_schema, cq_schema_components, query_schema


def test_cq_result_schema_exports() -> None:
    schema = cq_result_schema()
    assert isinstance(schema, dict)
    assert "$ref" in schema
    defs = schema.get("$defs")
    assert isinstance(defs, dict)
    assert "CqResult" in defs


def test_query_schema_exports() -> None:
    schema = query_schema()
    assert isinstance(schema, dict)
    assert "$ref" in schema
    defs = schema.get("$defs")
    assert isinstance(defs, dict)
    assert "Query" in defs


def test_cq_schema_components_exports() -> None:
    schema_docs, components = cq_schema_components()
    assert isinstance(schema_docs, tuple)
    assert len(schema_docs) >= 2
    assert isinstance(components, dict)
    assert components
    component_keys = set(components)
    assert "RunPlan" in component_keys or "RunStep" in component_keys
    assert "TreeSitterArtifactBundleV1" in component_keys
