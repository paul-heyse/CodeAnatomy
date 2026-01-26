"""SQLGlot AST builders for view-first pipelines."""

from __future__ import annotations

from collections.abc import Sequence

from sqlglot_tools.compat import Expression, exp


def select_all(table: str) -> Expression:
    """Return a SELECT * FROM table expression.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot expression selecting all columns from the table.
    """
    return exp.select("*").from_(table)


def union_all(tables: Sequence[str]) -> Expression:
    """Return a UNION ALL expression over the provided tables.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot expression combining all tables with UNION ALL.

    Raises
    ------
    ValueError
        Raised when no table names are provided.
    """
    if not tables:
        msg = "union_all requires at least one table name."
        raise ValueError(msg)
    iterator = iter(tables)
    combined = select_all(next(iterator))
    for name in iterator:
        combined = exp.Union(this=combined, expression=select_all(name), all=True)
    return combined


def relation_output_sql() -> Expression:
    """Return the SQLGlot AST for relation_output_v1.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot expression defining the relation output view.
    """
    from relspec.relationship_sql import build_relation_output_sql

    return build_relation_output_sql()


__all__ = ["relation_output_sql", "select_all", "union_all"]
