"""Parity checks for CPG registry table round-trips."""

from __future__ import annotations

from cpg.registry_bundles import bundle_catalog
from cpg.registry_fields import field_catalog
from cpg.registry_readers import (
    bundle_catalog_from_table,
    dataset_rows_from_table,
    field_catalog_from_table,
    registry_templates_from_table,
)
from cpg.registry_rows import DATASET_ROWS
from cpg.registry_tables import (
    bundle_catalog_table,
    dataset_rows_table,
    field_catalog_table,
    registry_templates_table,
)
from cpg.registry_templates import registry_templates
from storage.deltalake.cpg_registry import build_registry_tables


def test_dataset_rows_round_trip() -> None:
    """Round-trip dataset rows through the registry table."""
    table = dataset_rows_table()
    decoded = dataset_rows_from_table(table)
    assert decoded == DATASET_ROWS


def test_field_catalog_round_trip() -> None:
    """Round-trip field catalog entries through the registry table."""
    table = field_catalog_table()
    decoded = field_catalog_from_table(table)
    assert decoded == field_catalog()


def test_bundle_catalog_round_trip() -> None:
    """Round-trip bundle catalog entries through the registry table."""
    field_map = field_catalog_from_table(field_catalog_table())
    table = bundle_catalog_table()
    decoded = bundle_catalog_from_table(table, field_catalog=field_map)
    assert decoded == bundle_catalog()


def test_registry_templates_round_trip() -> None:
    """Round-trip registry templates through the registry table."""
    table = registry_templates_table()
    decoded = registry_templates_from_table(table)
    assert decoded == registry_templates()


def test_cpg_registry_tables_include_catalogs() -> None:
    """Ensure registry tables include the catalog entries."""
    tables = build_registry_tables().tables
    expected = {
        "cpg_dataset_rows",
        "cpg_field_catalog",
        "cpg_bundle_catalog",
        "cpg_registry_templates",
    }
    assert expected.issubset(set(tables))
