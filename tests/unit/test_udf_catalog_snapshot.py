"""Tests for UdfCatalog snapshot hashing behavior."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.udf.metadata import DataFusionUdfSpec, UdfCatalog, UdfCatalogAdapter


def _sample_spec(func_id: str) -> DataFusionUdfSpec:
    return DataFusionUdfSpec(
        func_id=func_id,
        engine_name=func_id,
        kind="scalar",
        input_types=(),
        return_type=pa.int64(),
    )


def test_udf_catalog_snapshot_includes_hash() -> None:
    catalog = UdfCatalog(udf_specs={"alpha": _sample_spec("alpha")})
    adapter = UdfCatalogAdapter(catalog, function_factory_hash="hash-1")

    snapshot = adapter.snapshot()

    assert snapshot.get("function_factory_hash") == "hash-1"
    specs = snapshot.get("specs")
    assert isinstance(specs, dict)
    assert "alpha" in specs


def test_udf_catalog_restore_rejects_hash_mismatch() -> None:
    catalog = UdfCatalog()
    adapter = UdfCatalogAdapter(catalog, function_factory_hash="hash-1")
    snapshot = {
        "specs": {"alpha": _sample_spec("alpha")},
        "function_factory_hash": "hash-2",
    }

    with pytest.raises(ValueError, match="FunctionFactory hash mismatch"):
        adapter.restore(snapshot)


def test_udf_catalog_restore_accepts_hash_match() -> None:
    catalog = UdfCatalog()
    adapter = UdfCatalogAdapter(catalog, function_factory_hash="hash-1")
    snapshot = {
        "specs": {"alpha": _sample_spec("alpha")},
        "function_factory_hash": "hash-1",
    }

    adapter.restore(snapshot)

    assert "alpha" in catalog.custom_specs()
