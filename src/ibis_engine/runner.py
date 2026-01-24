"""Runner helpers for Ibis plans."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from datafusion_engine.runtime import (
        AdapterExecutionPolicy,
        DataFusionRuntimeProfile,
        ExecutionLabel,
    )

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from ibis.expr.types import Table, Value

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import (
    IbisCompilerBackend,
    datafusion_to_async_batches,
    datafusion_to_reader,
    datafusion_to_table,
    ibis_to_datafusion_dual_lane,
)
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.schema_introspection import SchemaIntrospector
from ibis_engine.expr_compiler import OperationSupportBackend, unsupported_operations
from ibis_engine.plan import IbisPlan
from obs.datafusion_runs import tracked_run


def _portable_plan(plan: IbisPlan) -> IbisPlan:
    unbind = getattr(plan.expr, "unbind", None)
    if callable(unbind):
        unbound = unbind()
        if isinstance(unbound, Table):
            return IbisPlan(expr=unbound, ordering=plan.ordering)
    return plan


def _datafusion_run_label(
    execution: DataFusionExecutionOptions,
    *,
    operation: str,
) -> str:
    label = execution.execution_label
    if label is None:
        return f"datafusion_{operation}"
    base = label.output_dataset or label.task_name
    if base:
        return f"{base}_{operation}"
    return f"datafusion_{operation}"


def _datafusion_run_metadata(
    execution: DataFusionExecutionOptions,
    *,
    operation: str,
) -> Mapping[str, object]:
    label = execution.execution_label
    payload: dict[str, object] = {"operation": operation}
    if label is not None:
        payload["task_name"] = label.task_name
        payload["output"] = label.output_dataset
    return payload


def _record_tracing_snapshot(
    execution: DataFusionExecutionOptions,
    *,
    run_id: str,
) -> None:
    runtime_profile = execution.runtime_profile
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    from datafusion_engine.runtime import collect_datafusion_traces

    traces = collect_datafusion_traces(runtime_profile)
    if traces is None:
        return
    runtime_profile.diagnostics_sink.record_artifact(
        "datafusion_traces_v1",
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "run_id": run_id,
            "traces": dict(traces),
        },
    )


def _record_metrics_snapshot(
    execution: DataFusionExecutionOptions,
    *,
    run_id: str,
) -> None:
    runtime_profile = execution.runtime_profile
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    from datafusion_engine.runtime import collect_datafusion_metrics

    metrics = collect_datafusion_metrics(runtime_profile)
    if metrics is None:
        return
    runtime_profile.diagnostics_sink.record_artifact(
        "datafusion_metrics_v1",
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "run_id": run_id,
            "metrics": dict(metrics),
        },
    )


@dataclass(frozen=True)
class DataFusionExecutionOptions:
    """Execution options for DataFusion-backed Ibis plans."""

    backend: IbisCompilerBackend
    ctx: SessionContext
    options: DataFusionCompileOptions | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    runtime_profile_hash: str | None = None
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
    param_mode: str | None = None
    param_signature: str | None = None
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
            "param_mode": policy.param_mode,
            "param_signature": policy.param_signature,
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
    params = _validate_plan_params(execution.params) if execution is not None else None
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
    portable_plan = _portable_plan(plan)
    if _force_bridge(options):
        df = plan_to_datafusion(
            portable_plan,
            execution=execution,
            params=params,
        )
        return datafusion_to_table(df, ordering=portable_plan.ordering)
    missing = _missing_ops(
        plan,
        execution=execution,
    )
    if missing:
        msg = f"Unsupported Ibis operations: {', '.join(missing)}."
        raise ValueError(msg)
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
    validated_params = _validate_plan_params(effective_params)
    return plan.to_reader(batch_size=batch_size, params=validated_params)


def _validate_plan_params(
    params: Mapping[Value, object] | Mapping[str, object] | None,
) -> Mapping[Value, object] | None:
    if params is None:
        return None
    if not params:
        return {}
    if all(isinstance(key, Value) for key in params):
        return cast("Mapping[Value, object]", params)
    msg = "Plan execution expects parameter bindings keyed by Ibis values."
    raise ValueError(msg)


def validate_plan_params(
    params: Mapping[Value, object] | Mapping[str, object] | None,
) -> Mapping[Value, object] | None:
    """Return validated parameter bindings for plan execution.

    Parameters
    ----------
    params:
        Parameter bindings keyed by Ibis values or names.

    Returns
    -------
    Mapping[Value, object] | None
        Validated parameter bindings or None.
    """
    return _validate_plan_params(params)


async def async_stream_plan(
    plan: IbisPlan,
    *,
    batch_size: int | None = None,
    params: Mapping[Value, object] | None = None,
    execution: IbisPlanExecutionOptions | None = None,
) -> AsyncIterator[pa.RecordBatch]:
    """Return an async iterator over RecordBatches for an Ibis plan.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches produced from the plan.
    """
    effective_params = params
    if execution is not None and execution.params is not None:
        effective_params = execution.params
    if execution is not None:
        cache_policy = execution.cache_policy
        if cache_policy is not None:
            _record_cache_event(cache_policy, mode="async_stream")
        if cache_policy is not None and cache_policy.enabled:
            with plan.cache() as cached:
                cached_execution = replace(execution, cache_policy=None)
                async for batch in async_stream_plan(
                    cached,
                    batch_size=batch_size,
                    params=effective_params,
                    execution=cached_execution,
                ):
                    yield batch
                return
    if execution is not None and execution.datafusion is not None:
        df = plan_to_datafusion(
            plan,
            execution=execution.datafusion,
            params=effective_params,
        )
        async for batch in datafusion_to_async_batches(df, ordering=plan.ordering):
            yield batch
        return
    validated_params = _validate_plan_params(effective_params)
    reader = plan.to_reader(batch_size=batch_size, params=validated_params)
    for batch in reader:
        yield batch


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
    portable_plan = _portable_plan(plan)
    if _force_bridge(options):
        df = plan_to_datafusion(
            portable_plan,
            execution=execution,
            params=params,
        )
        return datafusion_to_reader(df, ordering=portable_plan.ordering)
    missing = _missing_ops(plan, execution=execution)
    if missing:
        msg = f"Unsupported Ibis operations: {', '.join(missing)}."
        raise ValueError(msg)
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
        execution=execution,
        params=params,
    )
    portable_plan = _portable_plan(plan)
    runtime_profile = execution.runtime_profile
    if (
        runtime_profile is None
        or runtime_profile.diagnostics_sink is None
        or options.run_id is not None
    ):
        result = ibis_to_datafusion_dual_lane(
            portable_plan.expr,
            backend=execution.backend,
            ctx=execution.ctx,
            options=options,
        )
        return result.df
    label = _datafusion_run_label(execution, operation="compile")
    metadata = _datafusion_run_metadata(execution, operation="compile")
    with tracked_run(label=label, sink=runtime_profile.diagnostics_sink, metadata=metadata) as run:
        result = ibis_to_datafusion_dual_lane(
            portable_plan.expr,
            backend=execution.backend,
            ctx=execution.ctx,
            options=replace(options, run_id=run.run_id),
        )
        _record_metrics_snapshot(execution, run_id=run.run_id)
        _record_tracing_snapshot(execution, run_id=run.run_id)
        return result.df


def _resolve_options(
    options: DataFusionCompileOptions | None,
    *,
    execution: DataFusionExecutionOptions,
    params: Mapping[Value, object] | None,
) -> DataFusionCompileOptions:
    from datafusion_engine.runtime import (
        apply_execution_label,
        apply_execution_policy,
        sql_options_for_profile,
    )

    resolved = options
    if resolved is None:
        if execution.runtime_profile is not None:
            resolved = execution.runtime_profile.compile_options(
                params=params,
                execution_policy=execution.execution_policy,
                execution_label=execution.execution_label,
            )
        else:
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
        if resolved.runtime_profile is None:
            resolved = replace(resolved, runtime_profile=execution.runtime_profile)
        resolved = apply_execution_label(
            resolved,
            execution_label=execution.execution_label,
            explain_sink=execution.runtime_profile.labeled_explains,
        )
    resolved = apply_execution_policy(
        resolved,
        execution_policy=execution.execution_policy,
    )
    if (
        resolved.schema_map is None
        and execution.runtime_profile is not None
        and execution.runtime_profile.enable_information_schema
    ):
        introspector = SchemaIntrospector(
            execution.ctx,
            sql_options=sql_options_for_profile(execution.runtime_profile),
        )
        schema_map = introspector.schema_map()
        schema_map_hash: str | None = None
        try:
            schema_map_hash = introspector.schema_map_fingerprint()
        except (RuntimeError, TypeError, ValueError):
            schema_map_hash = None
        resolved = replace(
            resolved,
            schema_map=schema_map,
            schema_map_hash=schema_map_hash,
        )
    return resolved


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
            "task_name": label.task_name,
            "output": label.output_dataset,
        }
        if label is not None
        else None,
    }
    profile.diagnostics_sink.record_artifact("ibis_support_matrix_v1", payload)


def _force_bridge(options: DataFusionCompileOptions) -> bool:
    return options.capture_explain or options.explain_hook is not None
