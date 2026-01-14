"""Runner helpers for Ibis plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Value

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import (
    IbisCompilerBackend,
    ibis_plan_to_datafusion,
    ibis_plan_to_table,
)
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.plan import IbisPlan


@dataclass(frozen=True)
class DataFusionExecutionOptions:
    """Execution options for DataFusion-backed Ibis plans."""

    backend: IbisCompilerBackend
    ctx: SessionContext
    options: DataFusionCompileOptions | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass(frozen=True)
class IbisPlanExecutionOptions:
    """Execution options for Ibis plans."""

    params: Mapping[Value, object] | None = None
    datafusion: DataFusionExecutionOptions | None = None


def materialize_plan(
    plan: IbisPlan,
    *,
    execution: IbisPlanExecutionOptions | None = None,
) -> TableLike:
    """Materialize an Ibis plan to an Arrow table.

    Returns
    -------
    TableLike
        Arrow table with ordering metadata applied when available.
    """
    if execution is not None and execution.datafusion is not None:
        options = _resolve_options(
            execution.datafusion.options,
            params=execution.params,
            runtime_profile=execution.datafusion.runtime_profile,
        )
        return ibis_plan_to_table(
            plan,
            backend=execution.datafusion.backend,
            ctx=execution.datafusion.ctx,
            options=options,
        )
    params = execution.params if execution is not None else None
    return plan.to_table(params=params)


def stream_plan(
    plan: IbisPlan,
    *,
    batch_size: int | None = None,
    params: Mapping[Value, object] | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        RecordBatchReader with ordering metadata applied when available.
    """
    return plan.to_reader(batch_size=batch_size, params=params)


def plan_to_datafusion(
    plan: IbisPlan,
    *,
    execution: DataFusionExecutionOptions,
    params: Mapping[Value, object] | None = None,
) -> DataFrame:
    """Return a DataFusion DataFrame for an Ibis plan.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFusion DataFrame for the Ibis expression.
    """
    options = _resolve_options(
        execution.options,
        params=params,
        runtime_profile=execution.runtime_profile,
    )
    return ibis_plan_to_datafusion(
        plan,
        backend=execution.backend,
        ctx=execution.ctx,
        options=options,
    )


def _resolve_options(
    options: DataFusionCompileOptions | None,
    *,
    params: Mapping[Value, object] | None,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionCompileOptions:
    if options is None:
        if runtime_profile is not None:
            return runtime_profile.compile_options(params=params)
        return DataFusionCompileOptions(params=params)
    if params is None or options.params is not None:
        return options
    return replace(options, params=params)
