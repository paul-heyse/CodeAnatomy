"""Ibis to DataFusion adapter helpers."""

from __future__ import annotations

from typing import Protocol

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table
from sqlglot import Expression

from datafusion_engine.df_builder import df_from_sqlglot


class SqlGlotCompiler(Protocol):
    """Protocol for backends exposing SQLGlot compilation."""

    def to_sqlglot(self, expr: Table) -> Expression:
        """Return a SQLGlot expression."""
        ...


class IbisCompilerBackend(Protocol):
    """Protocol for Ibis backends exposing a SQLGlot compiler."""

    compiler: SqlGlotCompiler


def ibis_to_datafusion(
    expr: Table,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
) -> DataFrame:
    """Compile an Ibis expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the expression.
    """
    sg_expr = backend.compiler.to_sqlglot(expr)
    return df_from_sqlglot(ctx, sg_expr)
