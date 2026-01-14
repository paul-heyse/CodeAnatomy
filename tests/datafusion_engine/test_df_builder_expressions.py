"""Expression coverage tests for SQLGlot -> DataFusion translation."""

from __future__ import annotations

import pytest
from sqlglot import parse_one

pytest.importorskip("datafusion")

from datafusion import SessionContext

from datafusion_engine.df_builder import df_from_sqlglot


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
        {"a": 1, "b": 2, "c": None, "name": "Alpha", "arr": [1, 2], "s": {"field": "x"}},
        {"a": 2, "b": 1, "c": 5, "name": "beta", "arr": [3, 4], "s": {"field": "y"}},
        {"a": 2, "b": 1, "c": None, "name": "ALPHA", "arr": [5], "s": {"field": "z"}},
    ]
    df = context.from_pylist(rows)
    context.register_table("t", df)
    return context


def test_scalar_expression_translation(ctx: SessionContext) -> None:
    """Translate arithmetic, case/coalesce, and concat expressions."""
    expr = parse_one(
        "SELECT "
        "a + b AS total, "
        "a - b AS diff, "
        "a * b AS prod, "
        "a / b AS ratio, "
        "COALESCE(c, 0) AS c0, "
        "CASE WHEN a > 1 THEN b ELSE a END AS chosen, "
        "CONCAT(name, '_x') AS tag, "
        "name || '_y' AS tag2 "
        "FROM t"
    )
    table = df_from_sqlglot(ctx, expr).to_arrow_table()
    assert table.column_names == [
        "total",
        "diff",
        "prod",
        "ratio",
        "c0",
        "chosen",
        "tag",
        "tag2",
    ]
    assert table.column("total").to_pylist() == [3, 3, 3]
    assert table.column("diff").to_pylist() == [-1, 1, 1]
    assert table.column("prod").to_pylist() == [2, 2, 2]
    assert table.column("ratio").to_pylist() == pytest.approx([0.5, 2.0, 2.0])
    assert table.column("c0").to_pylist() == [0, 5, 0]
    assert table.column("chosen").to_pylist() == [1, 1, 1]
    assert table.column("tag").to_pylist() == ["Alpha_x", "beta_x", "ALPHA_x"]
    assert table.column("tag2").to_pylist() == ["Alpha_y", "beta_y", "ALPHA_y"]


def test_predicate_translation(ctx: SessionContext) -> None:
    """Translate LIKE/ILIKE, IN, and BETWEEN predicates."""
    like_expr = parse_one("SELECT name FROM t WHERE name LIKE 'Al%' ORDER BY name")
    like_table = df_from_sqlglot(ctx, like_expr).to_arrow_table()
    assert like_table.column("name").to_pylist() == ["Alpha"]

    ilike_expr = parse_one("SELECT name FROM t WHERE name ILIKE 'al%' ORDER BY name")
    ilike_table = df_from_sqlglot(ctx, ilike_expr).to_arrow_table()
    assert set(ilike_table.column("name").to_pylist()) == {"Alpha", "ALPHA"}

    in_expr = parse_one("SELECT a FROM t WHERE a IN (1)")
    in_table = df_from_sqlglot(ctx, in_expr).to_arrow_table()
    assert in_table.column("a").to_pylist() == [1]

    between_expr = parse_one("SELECT a FROM t WHERE a BETWEEN 2 AND 2 ORDER BY a")
    between_table = df_from_sqlglot(ctx, between_expr).to_arrow_table()
    assert between_table.column("a").to_pylist() == [2, 2]


def test_bracket_access_translation(ctx: SessionContext) -> None:
    """Translate array and struct bracket access."""
    expr = parse_one("SELECT arr[1] AS first, s['field'] AS field FROM t ORDER BY a")
    table = df_from_sqlglot(ctx, expr).to_arrow_table()
    assert table.column("first").to_pylist() == [1, 3, 5]
    assert table.column("field").to_pylist() == ["x", "y", "z"]


def test_aggregate_distinct_and_filter(ctx: SessionContext) -> None:
    """Translate DISTINCT aggregates and FILTER clauses."""
    expr = parse_one(
        "SELECT "
        "COUNT(DISTINCT a) AS distinct_a, "
        "SUM(b) FILTER (WHERE b > 1) AS sum_b "
        "FROM t"
    )
    table = df_from_sqlglot(ctx, expr).to_arrow_table()
    assert table.column("distinct_a").to_pylist() == [2]
    assert table.column("sum_b").to_pylist() == [2]

    distinct_expr = parse_one("SELECT DISTINCT a FROM t ORDER BY a")
    distinct_table = df_from_sqlglot(ctx, distinct_expr).to_arrow_table()
    assert distinct_table.column("a").to_pylist() == [1, 2]


def test_join_using_translation(ctx: SessionContext) -> None:
    """Translate JOIN USING with qualifier stripping."""
    other_rows = [{"a": 1, "val": "one"}, {"a": 2, "val": "two"}]
    other_df = ctx.from_pylist(other_rows)
    ctx.register_table("u", other_df)
    expr = parse_one("SELECT a, val FROM t JOIN u USING (a) ORDER BY a, val")
    table = df_from_sqlglot(ctx, expr).to_arrow_table()
    assert table.column("a").to_pylist() == [1, 2, 2]
    assert table.column("val").to_pylist() == ["one", "two", "two"]
