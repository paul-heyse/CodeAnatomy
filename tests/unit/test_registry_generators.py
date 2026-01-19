"""Parity checks for normalize/extract registry generators."""

from __future__ import annotations

from storage.deltalake.extract_registry import build_registry_tables as build_extract_tables
from storage.deltalake.normalize_registry import build_registry_tables as build_normalize_tables


def test_normalize_registry_tables_include_core_specs() -> None:
    """Ensure normalize registry exports core spec tables."""
    tables = build_normalize_tables().tables
    expected = {"schema_fields", "schema_constraints", "normalize_rule_families"}
    assert expected.issubset(set(tables))


def test_extract_registry_tables_include_dataset_specs() -> None:
    """Ensure extract registry exports dataset spec tables."""
    tables = build_extract_tables().tables
    assert "extract_datasets" in tables
