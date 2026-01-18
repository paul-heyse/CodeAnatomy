"""Runner helpers for Ibis plans."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import cast

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table, Value
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
from ibis_engine.sql_bridge import execute_raw_sql, sql_ingest_artifacts

logger = logging.getLogger(__name__)


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
    raw_expr = execute_raw_sql(
        backend,
        sql=sql,
        sqlglot_expr=sg_expr,
        schema=plan.expr.schema(),
    )
    if options.sql_ingest_hook is not None:
        options.sql_ingest_hook(
            sql_ingest_artifacts(
                sql,
                expr=raw_expr,
                sqlglot_expr=sg_expr,
                dialect=options.dialect,
            ).payload()
        )
    return IbisPlan(expr=raw_expr, ordering=plan.ordering)


def _portable_plan(plan: IbisPlan) -> IbisPlan:
    unbind = getattr(plan.expr, "unbind", None)
    if callable(unbind):
        unbound = unbind()
        if isinstance(unbound, Table):
            return IbisPlan(expr=unbound, ordering=plan.ordering)
    return plan


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
    runtime_profile_hash: str | None = None
    allow_fallback: bool = True
    execution_policy: AdapterExecutionPolicy | None = None
    execution_label: ExecutionLabel | None = None
    probe_capabilities: bool = True


@dataclass(frozen=True)
class IbisPlanExecutionOptions:
    """Execution options for Ibis plans."""

    params: Mapping[Value, object] | None = None
    datafusion: DataFusionExecutionOptions | None = None
    cache_policy: IbisCachePolicy | None = None


@dataclass(frozen=True)
class IbisCachePolicy:
    """Cache policy for Ibis plan execution."""

    enabled: bool = False
    reason: str | None = None
    writer_strategy: str | None = None
    reporter: Callable[[Mapping[str, object]], None] | None = None


def _record_cache_event(policy: IbisCachePolicy, *, mode: str) -> None:
    if policy.reporter is None:
        return
    policy.reporter(
        {
            "mode": mode,
            "enabled": policy.enabled,
            "reason": policy.reason,
            "writer_strategy": policy.writer_strategy,
        }
    )


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
    if execution is not None:
        cache_policy = execution.cache_policy
        if cache_policy is not None:
            _record_cache_event(cache_policy, mode="materialize")
        if cache_policy is not None and cache_policy.enabled:
            with plan.cache() as cached:
                cached_execution = replace(execution, cache_policy=None)
                return materialize_plan(cached, execution=cached_execution)
        if execution.datafusion is not None:
            return _materialize_plan_datafusion(
                plan,
                execution=execution.datafusion,
                params=execution.params,
            )
    params = execution.params if execution is not None else None
    return plan.to_table(params=params)


def _materialize_plan_datafusion(
    plan: IbisPlan,
    *,
    execution: DataFusionExecutionOptions,
    params: Mapping[Value, object] | None,
) -> TableLike:
    options = _resolve_options(
        execution.options,
        execution=execution,
        params=params,
    )
    if options.force_sql:
        raw_table = _try_raw_sql_table(
            plan,
            backend=execution.backend,
            options=options,
        )
        if raw_table is not None:
            return raw_table
        if not execution.allow_fallback:
            msg = "Raw SQL execution failed and fallback is disabled."
            raise ValueError(msg)
    portable_plan = _portable_plan(plan)
    if _force_bridge(options):
        return ibis_plan_to_table(
            portable_plan,
            backend=execution.backend,
            ctx=execution.ctx,
            options=options,
        )
    missing = _missing_ops(
        plan,
        execution=execution,
    )
    if not missing:
        try:
            return plan.to_table(params=params)
        except Exception as exc:
            if not execution.allow_fallback:
                raise
            logger.warning(
                "Ibis DataFusion execution failed; falling back to SQLGlot bridge: %s",
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
    return ibis_plan_to_table(
        portable_plan,
        backend=execution.backend,
        ctx=execution.ctx,
        options=options,
    )


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
    if execution is not None:
        cache_policy = execution.cache_policy
        if cache_policy is not None:
            _record_cache_event(cache_policy, mode="stream")
        if cache_policy is not None and cache_policy.enabled:
            with plan.cache() as cached:
                cached_execution = replace(execution, cache_policy=None)
                return stream_plan(
                    cached,
                    batch_size=batch_size,
                    params=effective_params,
                    execution=cached_execution,
                )
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
        execution=execution,
        params=params,
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
    portable_plan = _portable_plan(plan)
    if _force_bridge(options):
        return ibis_plan_to_reader(
            portable_plan,
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
        portable_plan,
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
        execution=execution,
        params=params,
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
    execution: DataFusionExecutionOptions,
    params: Mapping[Value, object] | None,
) -> DataFusionCompileOptions:
    resolved = options
    if resolved is None:
        if execution.runtime_profile is not None:
            return execution.runtime_profile.compile_options(
                params=params,
                execution_policy=execution.execution_policy,
                execution_label=execution.execution_label,
            )
        resolved = DataFusionCompileOptions(params=params)
    elif params is not None and resolved.params is None:
        resolved = replace(resolved, params=params)
    if execution.runtime_profile is not None:
        if resolved.plan_cache is None:
            resolved = replace(resolved, plan_cache=execution.runtime_profile.plan_cache)
        if resolved.profile_hash is None:
            resolved = replace(
                resolved,
                profile_hash=(
                    execution.runtime_profile_hash or execution.runtime_profile.settings_hash()
                ),
            )
        resolved = apply_execution_label(
            resolved,
            execution_label=execution.execution_label,
            fallback_sink=execution.runtime_profile.labeled_fallbacks,
            explain_sink=execution.runtime_profile.labeled_explains,
        )
    return apply_execution_policy(
        resolved,
        execution_policy=execution.execution_policy,
        execution_label=execution.execution_label,
    )


def _missing_ops(
    plan: IbisPlan,
    *,
    execution: DataFusionExecutionOptions,
) -> tuple[str, ...]:
    if not execution.probe_capabilities:
        return ()
    backend = cast("OperationSupportBackend", execution.backend)
    missing = unsupported_operations(plan.expr, backend=backend)
    _record_support_matrix(execution, missing=missing)
    return missing


def _record_support_matrix(
    execution: DataFusionExecutionOptions,
    *,
    missing: tuple[str, ...],
) -> None:
    profile = execution.runtime_profile
    if profile is None or profile.diagnostics_sink is None:
        return
    label = execution.execution_label
    payload = {
        "backend": type(execution.backend).__name__,
        "missing": list(missing),
        "execution_label": {
            "rule": label.rule_name,
            "output": label.output_dataset,
        }
        if label is not None
        else None,
    }
    profile.diagnostics_sink.record_artifact("ibis_support_matrix_v1", payload)


def _force_bridge(options: DataFusionCompileOptions) -> bool:
    return options.force_sql or options.capture_explain or options.explain_hook is not None
