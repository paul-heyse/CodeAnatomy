"""Tests for dataset-spec module split."""

from __future__ import annotations

from schema_spec import dataset_spec


def test_dataset_spec_module_has_primary_contract_type() -> None:
    """Dataset spec module exports primary DatasetSpec contract."""
    assert hasattr(dataset_spec, "DatasetSpec")


def test_dataset_spec_no_longer_exports_split_scan_and_contract_symbols() -> None:
    """Moved split-module symbols should not be exported by dataset_spec facade."""
    moved_symbols = (
        "DataFusionScanOptions",
        "DeltaScanOptions",
        "ParquetColumnOptions",
        "ScanPolicyConfig",
        "ScanPolicyDefaults",
        "DeltaScanPolicyDefaults",
        "apply_scan_policy",
        "apply_delta_scan_policy",
        "ContractRow",
        "TableSchemaContract",
        "DedupeSpecSpec",
        "SortKeySpec",
    )
    for symbol in moved_symbols:
        assert not hasattr(dataset_spec, symbol)
