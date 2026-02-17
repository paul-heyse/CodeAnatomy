"""Tests for table-spec split module."""

from __future__ import annotations

import inspect

from schema_spec import dataset_spec, table_spec


def test_table_spec_aliases_dataset_contracts() -> None:
    """Table spec module aliases canonical dataset table contract types."""
    assert table_spec.TableSchemaSpec is dataset_spec.TableSchemaSpec


def test_table_spec_module_owns_table_build_helpers() -> None:
    """Table spec module owns local table build helper implementations."""
    source = inspect.getsource(table_spec)
    assert "def make_table_spec" in source
    assert "dataset_spec as _core" not in source
