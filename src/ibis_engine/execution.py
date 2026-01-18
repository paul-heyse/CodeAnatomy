"""Execution helpers for Ibis plans."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import (
    DataFusionExecutionOptions,
    IbisPlanExecutionOptions,
    materialize_plan,
    stream_plan,
)
from sqlglot_tools.bridge import IbisCompilerBackend


@dataclass(frozen=True)
class IbisExecutionContext:
    """Execution context for Ibis plans."""

    ctx: ExecutionContext
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    ibis_backend: BaseBackend | None = None
    params: Mapping[IbisValue, object] | None = None
    batch_size: int | None = None
    allow_fallback: bool = True
    probe_capabilities: bool = True

    def plan_options(self) -> IbisPlanExecutionOptions:
        """Build plan execution options for the current context.

        Returns
        -------
        IbisPlanExecutionOptions
            Execution options including DataFusion overrides when available.
        """
        if self.ibis_backend is None or self.ctx.runtime.datafusion is None:
            return IbisPlanExecutionOptions(params=self.params)
        runtime_profile = self.ctx.runtime.datafusion
        datafusion = DataFusionExecutionOptions(
            backend=cast("IbisCompilerBackend", self.ibis_backend),
            ctx=runtime_profile.session_context(),
            runtime_profile=runtime_profile,
            allow_fallback=self.allow_fallback,
            execution_policy=self.execution_policy,
            execution_label=self.execution_label,
            probe_capabilities=self.probe_capabilities,
        )
        return IbisPlanExecutionOptions(params=self.params, datafusion=datafusion)


def materialize_ibis_plan(plan: IbisPlan, *, execution: IbisExecutionContext) -> TableLike:
    """Materialize an Ibis plan using the execution context.

    Returns
    -------
    TableLike
        Materialized Arrow table.
    """
    return materialize_plan(plan, execution=execution.plan_options())


def stream_ibis_plan(
    plan: IbisPlan,
    *,
    execution: IbisExecutionContext,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        Streamed reader for the plan results.
    """
    return stream_plan(
        plan,
        batch_size=execution.batch_size,
        execution=execution.plan_options(),
    )


__all__ = ["IbisExecutionContext", "materialize_ibis_plan", "stream_ibis_plan"]
