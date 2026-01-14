"""Fallback coverage tests for SQLGlot -> DataFusion translation."""

from __future__ import annotations

import pytest
from sqlglot import parse_one

pytest.importorskip("datafusion")

from datafusion import SessionContext

from datafusion_engine.bridge import DataFusionFallbackEvent, df_from_sqlglot_or_sql
from datafusion_engine.compile_options import DataFusionCompileOptions


@pytest.fixture
def ctx() -> SessionContext:
    """Yield a SessionContext with sample data registered.

    Returns
    -------
    datafusion.SessionContext
        DataFusion session context.
    """
    context = SessionContext()
    rows = [
        {"a": 1, "b": 2},
        {"a": 2, "b": 1},
        {"a": 2, "b": 1},
    ]
    df = context.from_pylist(rows)
    context.register_table("t", df)
    return context


def test_fallback_emits_diagnostics(ctx: SessionContext) -> None:
    """Emit fallback diagnostics when SQL execution is required."""
    expr = parse_one("SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM t")
    events: list[DataFusionFallbackEvent] = []
    options = DataFusionCompileOptions(fallback_hook=events.append)
    df = df_from_sqlglot_or_sql(ctx, expr, options=options)
    table = df.to_arrow_table()
    assert table.column_names == ["rn"]
    assert events
    assert events[0].reason == "translation_error"
    assert "ROW_NUMBER" in events[0].sql
