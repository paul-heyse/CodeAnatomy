"""Runner helpers for Ibis plans."""

from __future__ import annotations

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import (
    DataFusionCompileOptions,
    IbisCompilerBackend,
    ibis_plan_to_datafusion,
    ibis_plan_to_table,
)
from ibis_engine.plan import IbisPlan


def materialize_plan(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend | None = None,
    ctx: SessionContext | None = None,
    options: DataFusionCompileOptions | None = None,
) -> TableLike:
    """Materialize an Ibis plan to an Arrow table.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when available.
    """
    if backend is not None and ctx is not None:
        return ibis_plan_to_table(
            plan,
            backend=backend,
            ctx=ctx,
            options=options,
        )
    return plan.to_table()


def stream_plan(plan: IbisPlan, *, batch_size: int | None = None) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        RecordBatchReader with ordering metadata applied when available.
    """
    return plan.to_reader(batch_size=batch_size)


def plan_to_datafusion(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    options: DataFusionCompileOptions | None = None,
) -> DataFrame:
    """Return a DataFusion DataFrame for an Ibis plan.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the Ibis expression.
    """
    return ibis_plan_to_datafusion(
        plan,
        backend=backend,
        ctx=ctx,
        options=options,
    )
