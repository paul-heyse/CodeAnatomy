# ruff: noqa: D103, INP001
"""Tests for listing/schema registration split modules."""

from __future__ import annotations

import inspect

from datafusion_engine.dataset import registration_listing, registration_schema


def test_registration_listing_contains_local_ddl_logic() -> None:
    source = inspect.getsource(registration_listing)
    assert "def _external_table_ddl" in source
    assert "def _register_dataset_with_context" in source
    assert "return _core._external_table_ddl" not in source
    assert "return _core._register_dataset_with_context" not in source
    assert "_register_listing_authority" in source


def test_registration_schema_contains_local_contract_logic() -> None:
    source = inspect.getsource(registration_schema)
    assert "def _validate_schema_contracts" in source
    assert "def _partition_schema_validation" in source
    assert "return _core._validate_schema_contracts" not in source
