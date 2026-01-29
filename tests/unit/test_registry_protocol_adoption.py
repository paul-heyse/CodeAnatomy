"""Registry protocol adoption tests for core registries."""

from __future__ import annotations

import pytest

pytest.importorskip("datafusion")

import pyarrow as pa

from datafusion_engine.dataset_registry import DatasetCatalog, DatasetLocation
from datafusion_engine.runtime import DataFusionViewRegistry
from datafusion_engine.schema_contracts import ColumnContract, ContractRegistry, SchemaContract
from datafusion_engine.view_artifacts import DataFusionViewArtifact
from utils.registry_protocol import Registry


def test_contract_registry_implements_protocol() -> None:
    """Ensure ContractRegistry matches the Registry protocol."""
    column = ColumnContract(name="id", arrow_type=pa.int32(), nullable=False)
    contract = SchemaContract(table_name="example", columns=(column,))
    registry = ContractRegistry()
    registry.register_contract(contract)
    assert isinstance(registry, Registry)
    assert registry.get("example") == contract
    assert "example" in registry
    assert list(iter(registry)) == ["example"]
    assert len(registry) == 1


def test_dataset_catalog_implements_protocol() -> None:
    """Ensure DatasetCatalog matches the Registry protocol."""
    location = DatasetLocation(path="s3://bucket/data")
    catalog = DatasetCatalog()
    catalog.register("dataset", location)
    assert isinstance(catalog, Registry)
    assert catalog.get("dataset") == location
    assert "dataset" in catalog
    assert list(iter(catalog)) == ["dataset"]
    assert len(catalog) == 1


def test_view_registry_implements_protocol() -> None:
    """Ensure DataFusionViewRegistry matches the Registry protocol."""
    schema = pa.schema([("id", pa.int32())])
    artifact = DataFusionViewArtifact(
        name="view",
        plan_fingerprint="fp",
        plan_task_signature="sig",
        schema=schema,
        required_udfs=(),
        referenced_tables=(),
    )
    registry = DataFusionViewRegistry()
    registry.register("view", artifact)
    assert isinstance(registry, Registry)
    assert registry.get("view") == artifact
    assert "view" in registry
    assert list(iter(registry)) == ["view"]
    assert len(registry) == 1
