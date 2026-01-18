"""Runner helpers for Ibis plans."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Protocol, cast

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value
from sqlglot import ErrorLevel

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import (
    IbisCompilerBackend,
    compile_sqlglot_expr,
    ibis_plan_to_datafusion,
    ibis_plan_to_reader,
    ibis_plan_to_table,
)
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.runtime import (
    AdapterExecutionPolicy,
    DataFusionRuntimeProfile,
    ExecutionLabel,
    apply_execution_label,
    apply_execution_policy,
)
from ibis_engine.expr_compiler import OperationSupportBackend, unsupported_operations
from ibis_engine.plan import IbisPlan

logger = logging.getLogger(__name__)


class _RawSqlBackend(Protocol):
    def raw_sql(self, query: str) -> object:
        """Execute a SQL string and return a backend result."""


def _raw_sql_plan(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    options: DataFusionCompileOptions,
) -> IbisPlan:
    if options.params:
        msg = "Raw SQL execution does not support parameter bindings."
        raise ValueError(msg)
    sg_expr = compile_sqlglot_expr(plan.expr, backend=backend, options=options)
    sql = sg_expr.sql(dialect=options.dialect, unsupported_level=ErrorLevel.RAISE)
    raw_backend = cast("_RawSqlBackend", backend)
    raw_expr = raw_backend.raw_sql(sql)
    if not isinstance(raw_expr, IbisTable):
        msg = "Raw SQL execution did not return an Ibis table."
        raise TypeError(msg)
    return IbisPlan(expr=raw_expr, ordering=plan.ordering)


def _try_raw_sql_table(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    options: DataFusionCompileOptions,
) -> TableLike | None:
    try:
        raw_plan = _raw_sql_plan(plan, backend=backend, options=options)
        return raw_plan.to_table(params=None)
    except (AttributeError, TypeError, ValueError, RuntimeError):
        return None


def _try_raw_sql_reader(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    options: DataFusionCompileOptions,
    batch_size: int | None,
) -> RecordBatchReaderLike | None:
    try:
        raw_plan = _raw_sql_plan(plan, backend=backend, options=options)
        return raw_plan.to_reader(batch_size=batch_size, params=None)
    except (AttributeError, TypeError, ValueError, RuntimeError):
        return None


@dataclass(frozen=True)
class DataFusionExecutionOptions:
    """Execution options for DataFusion-backed Ibis plans."""

    backend: IbisCompilerBackend
    ctx: SessionContext
    options: DataFusionCompileOptions | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    allow_fallback: bool = True
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    probe_capabilities: bool = True


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

    Raises
    ------
    ValueError
        Raised when unsupported operations are encountered without fallback.
    """
    if execution is not None and execution.datafusion is not None:
        options = _resolve_options(
            execution.datafusion.options,
            params=execution.params,
            runtime_profile=execution.datafusion.runtime_profile,
            execution_policy=execution.datafusion.execution_policy,
            execution_label=execution.datafusion.execution_label,
        )
        if options.force_sql:
            raw_table = _try_raw_sql_table(
                plan,
                backend=execution.datafusion.backend,
                options=options,
            )
            if raw_table is not None:
                return raw_table
            if not execution.datafusion.allow_fallback:
                msg = "Raw SQL execution failed and fallback is disabled."
                raise ValueError(msg)
        if _force_bridge(options):
            return ibis_plan_to_table(
                plan,
                backend=execution.datafusion.backend,
                ctx=execution.datafusion.ctx,
                options=options,
            )
        missing = _missing_ops(
            plan,
            execution=execution.datafusion,
        )
        if not missing:
            try:
                return plan.to_table(params=execution.params)
            except Exception as exc:
                if not execution.datafusion.allow_fallback:
                    raise
                logger.warning(
                    "Ibis DataFusion execution failed; falling back to SQLGlot bridge: %s",
                    exc,
                )
        elif not execution.datafusion.allow_fallback:
            msg = f"Unsupported Ibis operations: {', '.join(missing)}."
            raise ValueError(msg)
        else:
            logger.info(
                "Ibis backend lacks operations (%s); using SQLGlot bridge fallback.",
                ", ".join(missing),
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
    execution: IbisPlanExecutionOptions | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        RecordBatchReader with ordering metadata applied when available.
    """
    effective_params = params
    if execution is not None and execution.params is not None:
        effective_params = execution.params
    if execution is not None and execution.datafusion is not None:
        return _stream_plan_datafusion(
            plan,
            batch_size=batch_size,
            params=effective_params,
            execution=execution.datafusion,
        )
    return plan.to_reader(batch_size=batch_size, params=effective_params)


def _stream_plan_datafusion(
    plan: IbisPlan,
    *,
    batch_size: int | None,
    params: Mapping[Value, object] | None,
    execution: DataFusionExecutionOptions,
) -> RecordBatchReaderLike:
    options = _resolve_options(
        execution.options,
        params=params,
        runtime_profile=execution.runtime_profile,
        execution_policy=execution.execution_policy,
        execution_label=execution.execution_label,
    )
    if options.force_sql:
        raw_reader = _try_raw_sql_reader(
            plan,
            backend=execution.backend,
            options=options,
            batch_size=batch_size,
        )
        if raw_reader is not None:
            return raw_reader
        if not execution.allow_fallback:
            msg = "Raw SQL execution failed and fallback is disabled."
            raise ValueError(msg)
    if _force_bridge(options):
        return ibis_plan_to_reader(
            plan,
            backend=execution.backend,
            ctx=execution.ctx,
            options=options,
        )
    missing = _missing_ops(plan, execution=execution)
    if not missing:
        try:
            return plan.to_reader(batch_size=batch_size, params=params)
        except Exception as exc:
            if not execution.allow_fallback:
                raise
            logger.warning(
                "Ibis DataFusion streaming failed; falling back to SQLGlot bridge: %s",
                exc,
            )
    elif not execution.allow_fallback:
        msg = f"Unsupported Ibis operations: {', '.join(missing)}."
        raise ValueError(msg)
    else:
        logger.info(
            "Ibis backend lacks operations (%s); using SQLGlot bridge fallback.",
            ", ".join(missing),
        )
    return ibis_plan_to_reader(
        plan,
        backend=execution.backend,
        ctx=execution.ctx,
        options=options,
    )


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
        execution_policy=execution.execution_policy,
        execution_label=execution.execution_label,
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
    execution_policy: AdapterExecutionPolicy | None,
    execution_label: ExecutionLabel | None,
) -> DataFusionCompileOptions:
    resolved = options
    if resolved is None:
        if runtime_profile is not None:
            return runtime_profile.compile_options(
                params=params,
                execution_policy=execution_policy,
                execution_label=execution_label,
            )
        resolved = DataFusionCompileOptions(params=params)
    elif params is not None and resolved.params is None:
        resolved = replace(resolved, params=params)
    if runtime_profile is not None:
        if resolved.plan_cache is None:
            resolved = replace(resolved, plan_cache=runtime_profile.plan_cache)
        if resolved.profile_hash is None:
            resolved = replace(resolved, profile_hash=runtime_profile.settings_hash())
        resolved = apply_execution_label(
            resolved,
            execution_label=execution_label,
            fallback_sink=runtime_profile.labeled_fallbacks,
            explain_sink=runtime_profile.labeled_explains,
        )
    return apply_execution_policy(
        resolved,
        execution_policy=execution_policy,
        execution_label=execution_label,
    )


def _missing_ops(
    plan: IbisPlan,
    *,
    execution: DataFusionExecutionOptions,
) -> tuple[str, ...]:
    if not execution.probe_capabilities:
        return ()
    backend = cast("OperationSupportBackend", execution.backend)
    return unsupported_operations(plan.expr, backend=backend)


def _force_bridge(options: DataFusionCompileOptions) -> bool:
    return options.force_sql or options.capture_explain or options.explain_hook is not None
