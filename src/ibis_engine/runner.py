"""Execution helpers for Ibis plans with DataFusion integration."""

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
from arrowdsl.core.ordering import Ordering, OrderingLevel
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.compile_options import DataFusionCompileOptions
from datafusion_engine.execution_facade import DataFusionExecutionFacade, ExecutionResult
from datafusion_engine.schema_introspection import SchemaIntrospector, schema_map_fingerprint
from datafusion_engine.streaming_executor import StreamingExecutionResult
from ibis_engine.plan import IbisPlan
from obs.datafusion_runs import tracked_run
from sqlglot_tools.bridge import IbisCompilerBackend


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
    if runtime_profile is None:
        return
    from datafusion_engine.runtime import collect_datafusion_traces

    traces = collect_datafusion_traces(runtime_profile)
    if traces is None:
        return
    from datafusion_engine.diagnostics import recorder_for_profile

    recorder = recorder_for_profile(runtime_profile, operation_id="datafusion_traces_v1")
    if recorder is None:
        return
    recorder.record_artifact(
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
    if runtime_profile is None:
        return
    from datafusion_engine.runtime import collect_datafusion_metrics

    metrics = collect_datafusion_metrics(runtime_profile)
    if metrics is None:
        return
    from datafusion_engine.diagnostics import recorder_for_profile

    recorder = recorder_for_profile(runtime_profile, operation_id="datafusion_metrics_v1")
    if recorder is None:
        return
    recorder.record_artifact(
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

    Raises
    ------
    ValueError
        Raised when DataFusion execution options are missing.
    """
    if execution is None or execution.datafusion is None:
        msg = "DataFusion execution options are required for plan materialization."
        raise ValueError(msg)
    cache_policy = execution.cache_policy
    if cache_policy is not None:
        _record_cache_event(cache_policy, mode="materialize")
    if cache_policy is not None and cache_policy.enabled:
        with plan.cache() as cached:
            cached_execution = replace(execution, cache_policy=None)
            return materialize_plan(cached, execution=cached_execution)
    portable_plan = _portable_plan(plan)
    df = _datafusion_dataframe_for_plan(
        portable_plan,
        execution=execution.datafusion,
        params=execution.params,
    )
    return _datafusion_table_from_dataframe(df, ordering=portable_plan.ordering)


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

    Raises
    ------
    ValueError
        Raised when DataFusion execution options are missing.
    """
    if execution is None or execution.datafusion is None:
        msg = "DataFusion execution options are required for plan streaming."
        raise ValueError(msg)
    effective_params = params
    if execution.params is not None:
        effective_params = execution.params
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
    portable_plan = _portable_plan(plan)
    df = _datafusion_dataframe_for_plan(
        portable_plan,
        execution=execution.datafusion,
        params=effective_params,
    )
    return _datafusion_reader_from_dataframe(df, ordering=portable_plan.ordering)


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
    effective_execution = execution
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
        if cache_policy is not None:
            effective_execution = replace(execution, cache_policy=None)
    reader = stream_plan(
        plan,
        batch_size=batch_size,
        params=effective_params,
        execution=effective_execution,
    )
    for batch in reader:
        yield batch


def _datafusion_dataframe_for_plan(
    plan: IbisPlan,
    *,
    execution: DataFusionExecutionOptions,
    params: Mapping[Value, object] | None = None,
) -> DataFrame:
    options = _resolve_options(
        execution.options,
        execution=execution,
        params=params,
    )
    facade = DataFusionExecutionFacade(
        ctx=execution.ctx,
        runtime_profile=execution.runtime_profile,
        ibis_backend=execution.backend,
    )
    runtime_profile = execution.runtime_profile
    if (
        runtime_profile is None
        or runtime_profile.diagnostics_sink is None
        or options.run_id is not None
    ):
        result = facade.execute_expr(plan.expr, options=options)
        return _require_dataframe(result)
    label = _datafusion_run_label(execution, operation="compile")
    metadata = _datafusion_run_metadata(execution, operation="compile")
    from datafusion_engine.diagnostics import ensure_recorder_sink

    sink = ensure_recorder_sink(
        runtime_profile.diagnostics_sink,
        session_id=runtime_profile.context_cache_key(),
    )
    with tracked_run(label=label, sink=sink, metadata=metadata) as run:
        result = facade.execute_expr(plan.expr, options=replace(options, run_id=run.run_id))
        df = _require_dataframe(result)
        _record_metrics_snapshot(execution, run_id=run.run_id)
        _record_tracing_snapshot(execution, run_id=run.run_id)
        return df


def _require_dataframe(result: ExecutionResult) -> DataFrame:
    if result.dataframe is None:
        msg = f"Execution result is not a dataframe: {result.kind}."
        raise ValueError(msg)
    return result.dataframe


def _datafusion_reader_from_dataframe(
    df: DataFrame,
    *,
    ordering: Ordering | None,
) -> RecordBatchReaderLike:
    reader = StreamingExecutionResult(df=df).to_arrow_stream()
    return _apply_ordering(reader, ordering=ordering)


def _datafusion_table_from_dataframe(
    df: DataFrame,
    *,
    ordering: Ordering | None,
) -> TableLike:
    table = StreamingExecutionResult(df=df).to_table()
    if ordering is None or ordering.level == OrderingLevel.UNORDERED:
        return table
    spec = ordering_metadata_spec(ordering.level, keys=ordering.keys)
    return table.cast(spec.apply(table.schema))


def _apply_ordering(
    reader: RecordBatchReaderLike,
    *,
    ordering: Ordering | None,
) -> RecordBatchReaderLike:
    if ordering is None or ordering.level == OrderingLevel.UNORDERED:
        return reader
    spec = ordering_metadata_spec(ordering.level, keys=ordering.keys)
    return pa.RecordBatchReader.from_batches(spec.apply(reader.schema), reader)


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
            schema_map_hash = schema_map_fingerprint(introspector)
        except (RuntimeError, TypeError, ValueError):
            schema_map_hash = None
        resolved = replace(
            resolved,
            schema_map=schema_map,
            schema_map_hash=schema_map_hash,
        )
    return resolved


"""Runner helpers for Ibis plans."""
