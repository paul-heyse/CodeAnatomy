"""Ibis to DataFusion adapter helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table, Value

from datafusion_engine.bridge import IbisCompilerBackend
from datafusion_engine.bridge import ibis_to_datafusion as _ibis_to_datafusion
from datafusion_engine.compile_options import DataFusionCompileOptions


def ibis_to_datafusion(
    expr: Table,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    params: Mapping[str, object] | Mapping[Value, object] | None = None,
) -> DataFrame:
    """Compile an Ibis expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the expression.
    """
    options = DataFusionCompileOptions(params=params)
    return _ibis_to_datafusion(expr, backend=backend, ctx=ctx, options=options)
