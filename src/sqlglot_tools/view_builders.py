"""SQLGlot AST builders for view-first pipelines."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from sqlglot_tools.compat import Expression, exp
from sqlglot_tools.lineage import canonical_ast_fingerprint

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


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

def sqlglot_view_builder(
    expr: Expression,
    *,
    dialect: str = "datafusion",
) -> Callable[[SessionContext], DataFrame]:
    """Return a DataFusion builder that executes a SQLGlot AST.

    Parameters
    ----------
    expr
        SQLGlot expression representing the view.
    dialect
        SQL dialect used for SQL emission.

    Returns
    -------
    Callable[[SessionContext], DataFrame]
        Builder that executes the SQL in a DataFusion session.
    """
    sql = expr.sql(dialect=dialect)

    def _build(ctx: SessionContext) -> DataFrame:
        return ctx.sql(sql)

    return _build


def view_sql(expr: Expression, *, dialect: str = "datafusion") -> str:
    """Return deterministic SQL for a SQLGlot expression.

    Returns
    -------
    str
        SQL text rendered for the requested dialect.
    """
    return expr.sql(dialect=dialect)


def view_fingerprint(expr: Expression) -> str:
    """Return canonical fingerprint for a view expression.

    Returns
    -------
    str
        Canonical fingerprint derived from the SQLGlot AST.
    """
    return canonical_ast_fingerprint(expr)


__all__ = [
    "relation_output_sql",
    "select_all",
    "sqlglot_view_builder",
    "union_all",
    "view_fingerprint",
    "view_sql",
]
