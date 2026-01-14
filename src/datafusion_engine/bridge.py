"""Bridge helpers for Ibis/SQLGlot execution in DataFusion."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table as IbisTable
from sqlglot import Expression

from arrowdsl.core.context import Ordering, OrderingLevel
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.df_builder import df_from_sqlglot
from ibis_engine.plan import IbisPlan
from sqlglot_tools.bridge import IbisCompilerBackend, SchemaMapping, ibis_to_sqlglot
from sqlglot_tools.optimizer import normalize_expr


@dataclass(frozen=True)
class DataFusionCompileOptions:
    """Compilation options for DataFusion bridge execution."""

    schema_map: SchemaMapping | None = None
    optimize: bool = True
    cache: bool = False


def sqlglot_to_datafusion(
    expr: Expression,
    *,
    ctx: SessionContext,
) -> DataFrame:
    """Translate a SQLGlot expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame representing the expression.
    """
    return df_from_sqlglot(ctx, expr)


def ibis_to_datafusion(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Compile an Ibis expression into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the Ibis expression.
    """
    options = options or DataFusionCompileOptions()
    sg_expr = ibis_to_sqlglot(expr, backend=backend)
    if options.optimize:
        sg_expr = normalize_expr(sg_expr, schema=options.schema_map)
    df = df_from_sqlglot(ctx, sg_expr)
    return df.cache() if options.cache else df


def ibis_plan_to_datafusion(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Compile an Ibis plan into a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the plan.
    """
    return ibis_to_datafusion(plan.expr, backend=backend, ctx=ctx, options=options)


def datafusion_to_table(
    df: DataFrame,
    *,
    ordering: Ordering | None = None,
) -> TableLike:
    """Materialize a DataFusion DataFrame with optional ordering metadata.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when provided.
    """
    table = df.to_arrow_table()
    if ordering is None or ordering.level == OrderingLevel.UNORDERED:
        return table
    spec = ordering_metadata_spec(ordering.level, keys=ordering.keys)
    return table.cast(spec.apply(table.schema))


def ibis_plan_to_table(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> TableLike:
    """Compile an Ibis plan into a DataFusion-backed Arrow table.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when available.
    """
    df = ibis_plan_to_datafusion(plan, backend=backend, ctx=ctx, options=options)
    return datafusion_to_table(df, ordering=plan.ordering)


__all__ = [
    "DataFusionCompileOptions",
    "IbisCompilerBackend",
    "SchemaMapping",
    "datafusion_to_table",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "sqlglot_to_datafusion",
]
