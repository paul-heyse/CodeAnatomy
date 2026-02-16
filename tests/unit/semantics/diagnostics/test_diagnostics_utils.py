# ruff: noqa: D103, PLR6301
"""Tests for diagnostics utility helpers."""

from __future__ import annotations

from typing import Any, cast

import pyarrow as pa

from semantics.diagnostics._utils import empty_diagnostic_frame


class _FakeCtx:
    def from_arrow_table(self, table: pa.Table) -> pa.Table:
        return table


def test_empty_diagnostic_frame_builds_empty_table() -> None:
    schema = pa.schema([("x", pa.string())])
    table = empty_diagnostic_frame(cast("Any", _FakeCtx()), schema)
    assert cast("Any", table).num_rows == 0
    assert cast("Any", table).schema == schema
