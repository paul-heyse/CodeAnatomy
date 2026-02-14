"""Regression tests for nested type validation behavior."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from datafusion_engine.schema import nested_views as _nested_mod
from datafusion_engine.schema import registry as schema_registry

if TYPE_CHECKING:
    from datafusion import SessionContext


class _StubDataFrame:
    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def schema(self) -> pa.Schema:
        return self._schema


class _ExplodingContext:
    def table(self, _name: str) -> _StubDataFrame:
        msg = "table() should not be called for non-nested datasets"
        raise AssertionError(msg)


class _StubContext:
    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def table(self, _name: str) -> _StubDataFrame:
        return _StubDataFrame(self._schema)


def test_validate_nested_types_is_noop_for_non_nested_dataset() -> None:
    """Non-nested datasets should bypass nested-type validation entirely."""
    ctx = _ExplodingContext()

    schema_registry.validate_nested_types(cast("SessionContext", ctx), "file_line_index_v1")


def test_validate_nested_types_skips_when_schema_authority_is_missing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Missing derived nested schema authority should not raise."""
    ctx = _StubContext(pa.schema([pa.field("file_id", pa.int64())]))

    def _raise_missing(_name: str) -> pa.Schema:
        msg = "No derived extract schema available"
        raise KeyError(msg)

    monkeypatch.setattr(_nested_mod, "extract_schema_for", _raise_missing)

    with caplog.at_level(logging.WARNING):
        schema_registry.validate_nested_types(cast("SessionContext", ctx), "ast_nodes")

    assert any("no derived schema authority" in rec.message for rec in caplog.records)


def test_validate_nested_types_keeps_mismatch_diagnostics(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Expected-vs-actual nested mismatch should still emit warning diagnostics."""
    ctx = _StubContext(pa.schema([pa.field("actual_field", pa.string())]))
    expected = pa.schema([pa.field("expected_field", pa.string())])
    monkeypatch.setattr(_nested_mod, "extract_schema_for", lambda _name: expected)

    with caplog.at_level(logging.WARNING):
        schema_registry.validate_nested_types(cast("SessionContext", ctx), "ast_nodes")

    assert any("Nested type validation mismatch" in rec.message for rec in caplog.records)
