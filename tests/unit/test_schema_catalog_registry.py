"""Tests for DataFusion-backed schema authority."""

from __future__ import annotations

from datafusion_engine.runtime import dataset_schema_from_context
from datafusion_engine.schema_registry import nested_schema_for


def test_catalog_schema_resolves_nested_dataset() -> None:
    """Ensure nested dataset schemas resolve through DataFusion."""
    name = "cst_nodes"
    expected = nested_schema_for(name, allow_derived=True)
    resolved = dataset_schema_from_context(name)
    assert expected.equals(resolved)
