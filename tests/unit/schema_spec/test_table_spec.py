# ruff: noqa: D103
"""Tests for table-spec split module."""

from __future__ import annotations

import inspect

from schema_spec import dataset_spec, table_spec


def test_table_spec_aliases_dataset_contracts() -> None:
    assert table_spec.TableSchemaSpec is dataset_spec.TableSchemaSpec


def test_table_spec_module_owns_table_build_helpers() -> None:
    source = inspect.getsource(table_spec)
    assert "def make_table_spec" in source
    assert "dataset_spec as _core" not in source
