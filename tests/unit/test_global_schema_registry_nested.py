"""Tests for nested dataset decommissioning in the global schema registry."""

from __future__ import annotations

import pyarrow as pa

from cpg.registry_specs import dataset_schema as cpg_dataset_schema
from datafusion_engine.schema_registry import NESTED_DATASET_INDEX, nested_schema_for
from extract.registry_specs import dataset_schema as extract_dataset_schema
from normalize.registry_specs import dataset_schema as normalize_dataset_schema
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    SchemaRegistry,
    make_dataset_spec,
    prune_nested_dataset_specs,
    register_dataset_spec,
    table_spec_from_schema,
)


def test_global_registry_excludes_nested_dataset_names() -> None:
    """Ensure nested datasets are excluded from the global schema registry."""
    prune_nested_dataset_specs(GLOBAL_SCHEMA_REGISTRY)
    nested = set(NESTED_DATASET_INDEX)
    registered = set(GLOBAL_SCHEMA_REGISTRY.dataset_specs)
    assert not (nested & registered)


def test_register_dataset_spec_skips_nested_for_global_registry() -> None:
    """Ensure registering a nested dataset spec is ignored for the global registry."""
    name = "cst_nodes"
    GLOBAL_SCHEMA_REGISTRY.dataset_specs.pop(name, None)
    table_spec = table_spec_from_schema(name, pa.schema([("id", pa.int64())]))
    register_dataset_spec(make_dataset_spec(table_spec=table_spec))
    assert name not in GLOBAL_SCHEMA_REGISTRY.dataset_specs


def test_prune_nested_dataset_specs_removes_from_registry() -> None:
    """Ensure prune helper removes nested datasets from any registry."""
    name = "cst_nodes"
    registry = SchemaRegistry()
    table_spec = table_spec_from_schema(name, pa.schema([("id", pa.int64())]))
    registry.register_dataset(make_dataset_spec(table_spec=table_spec))
    assert name in registry.dataset_specs
    prune_nested_dataset_specs(registry)
    assert name not in registry.dataset_specs


def test_dataset_schema_uses_nested_schema_for_nested_names() -> None:
    """Ensure dataset_schema resolves nested schemas via DataFusion registry."""
    name = "cst_nodes"
    expected = nested_schema_for(name, allow_derived=True)
    assert expected.equals(extract_dataset_schema(name))
    assert expected.equals(normalize_dataset_schema(name))
    assert expected.equals(cpg_dataset_schema(name))
