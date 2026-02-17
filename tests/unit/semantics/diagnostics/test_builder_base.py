"""Tests for diagnostics batch-builder base."""

from __future__ import annotations

import pyarrow as pa

from semantics.diagnostics.builder_base import DiagnosticBatchBuilder

EXPECTED_ROW_COUNT = 2


def test_diagnostic_batch_builder_add_many_and_rows() -> None:
    """Builder accumulates rows and builds table with matching row count."""
    builder = DiagnosticBatchBuilder()
    builder.add_many([{"a": 1}, {"a": 2}])
    assert builder.rows() == [{"a": 1}, {"a": 2}]
    table = builder.build_table(schema=pa.schema([("a", pa.int64())]))
    assert table.num_rows == EXPECTED_ROW_COUNT
