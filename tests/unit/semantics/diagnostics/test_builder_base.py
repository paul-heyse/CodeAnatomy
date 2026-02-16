# ruff: noqa: D103, PLR2004
"""Tests for diagnostics batch-builder base."""

from __future__ import annotations

import pyarrow as pa

from semantics.diagnostics.builder_base import DiagnosticBatchBuilder


def test_diagnostic_batch_builder_add_many_and_rows() -> None:
    builder = DiagnosticBatchBuilder()
    builder.add_many([{"a": 1}, {"a": 2}])
    assert builder.rows() == [{"a": 1}, {"a": 2}]
    table = builder.build_table(schema=pa.schema([("a", pa.int64())]))
    assert table.num_rows == 2
