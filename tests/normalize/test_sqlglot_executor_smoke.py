"""Smoke tests for SQLGlot executor behavior in normalize flows."""

from __future__ import annotations

from sqlglot import executor


def test_sqlglot_executor_filter() -> None:
    """Execute a simple filter using the SQLGlot executor."""
    result = executor.execute(
        "SELECT x FROM t WHERE x > 1",
        tables={"t": [{"x": 1}, {"x": 3}]},
    )
    assert result.columns == ("x",)
    assert result.rows == [(3,)]
