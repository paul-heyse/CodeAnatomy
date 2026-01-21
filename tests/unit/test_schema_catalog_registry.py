"""Tests for the central schema catalog registry."""

from __future__ import annotations

from datafusion_engine.schema_registry import nested_schema_for
from schema_spec.catalog_registry import dataset_schema


def test_catalog_schema_resolves_nested_dataset() -> None:
    """Ensure nested dataset schemas resolve through DataFusion registry."""
    name = "cst_nodes"
    expected = nested_schema_for(name, allow_derived=True)
    resolved = dataset_schema(name)
    assert expected.equals(resolved)
