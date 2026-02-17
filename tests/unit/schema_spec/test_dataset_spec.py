"""Tests for dataset-spec module split."""

from __future__ import annotations

from schema_spec import dataset_spec


def test_dataset_spec_module_has_primary_contract_type() -> None:
    """Dataset spec module exports primary DatasetSpec contract."""
    assert hasattr(dataset_spec, "DatasetSpec")
