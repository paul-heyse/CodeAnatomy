"""Smoke tests for the SQLGlot executor."""

from __future__ import annotations

from sqlglot import executor


def test_sqlglot_executor_select_add() -> None:
    """Execute a simple projection using the SQLGlot executor."""
    result = executor.execute(
        "SELECT x + 1 AS y FROM t",
        tables={"t": [{"x": 1}, {"x": 2}]},
    )
    assert result.columns == ("y",)
    assert result.rows == [(2,), (3,)]
