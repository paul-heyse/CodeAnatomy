"""Adapterized execution helpers for Ibis plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.runner_types import (
    AdapterRunOptionsProto,
    PlanRunnerModule,
    PlanRunResultProto,
    plan_runner_module,
)
from config import AdapterMode
from datafusion_engine.runtime import AdapterExecutionPolicy
from ibis_engine.plan import IbisPlan


@dataclass(frozen=True)
class IbisAdapterExecution:
    """Adapter execution context for Ibis plans."""

    ctx: ExecutionContext
    adapter_mode: AdapterMode | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    params: Mapping[IbisValue, object] | None = None
    batch_size: int | None = None


def _plan_runner_module() -> PlanRunnerModule:
    return plan_runner_module()


def _adapter_run_options(
    execution: IbisAdapterExecution,
    *,
    prefer_reader: bool,
) -> AdapterRunOptionsProto:
    module = _plan_runner_module()
    params = cast("Mapping[object, object] | None", execution.params)
    return module.AdapterRunOptions(
        adapter_mode=execution.adapter_mode,
        prefer_reader=prefer_reader,
        execution_policy=execution.execution_policy,
        ibis_backend=execution.ibis_backend,
        ibis_params=params,
        ibis_batch_size=execution.batch_size,
    )


def _run_plan_adapter(
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    options: AdapterRunOptionsProto | None,
) -> PlanRunResultProto:
    module = _plan_runner_module()
    return module.run_plan_adapter(plan, ctx=ctx, options=options)


def materialize_ibis_plan(plan: IbisPlan, *, execution: IbisAdapterExecution) -> TableLike:
    """Materialize an Ibis plan via the adapter runner.

    Returns
    -------
    TableLike
        Materialized Arrow table.
    """
    result = _run_plan_adapter(
        plan,
        ctx=execution.ctx,
        options=_adapter_run_options(execution, prefer_reader=False),
    )
    return cast("TableLike", result.value)


def stream_ibis_plan(
    plan: IbisPlan,
    *,
    execution: IbisAdapterExecution,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan via the adapter runner.

    Returns
    -------
    RecordBatchReaderLike
        Streamed reader for the plan results.
    """
    result = _run_plan_adapter(
        plan,
        ctx=execution.ctx,
        options=_adapter_run_options(execution, prefer_reader=True),
    )
    if isinstance(result.value, pa.RecordBatchReader):
        return cast("RecordBatchReaderLike", result.value)
    table = cast("TableLike", result.value)
    if isinstance(table, pa.Table):
        arrow_table = cast("pa.Table", table)
    else:
        arrow_table = pa.Table.from_pylist(table.to_pylist(), schema=table.schema)
    batches = arrow_table.to_batches()
    return pa.RecordBatchReader.from_batches(arrow_table.schema, batches)


__all__ = ["IbisAdapterExecution", "materialize_ibis_plan", "stream_ibis_plan"]
