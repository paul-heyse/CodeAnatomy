"""Tests for the central schema catalog registry."""

from __future__ import annotations

from datafusion_engine.schema_registry import nested_schema_for
from schema_spec.catalog_registry import (
    dataset_schema,
    dataset_spec_catalog,
    schema_registry,
)


def test_catalog_schema_resolves_nested_dataset() -> None:
    """Ensure nested dataset schemas resolve through DataFusion registry."""
    name = "cst_nodes"
    expected = nested_schema_for(name, allow_derived=True)
    resolved = dataset_schema(name)
    assert expected.equals(resolved)


def test_catalog_includes_relation_output_spec() -> None:
    """Ensure the relation output spec is registered in the catalog."""
    assert "relation_output_v1" in dataset_spec_catalog().dataset_names()


def test_relationship_contract_specs_present_in_registry() -> None:
    """Ensure relationship dataset specs include contract specs."""
    registry = schema_registry()
    spec = registry.dataset_specs.get("rel_name_symbol_v1")
    assert spec is not None
    assert spec.contract_spec is not None
