"""Materialization helpers for plan outputs."""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import datafusion_from_arrow, datafusion_write_parquet
from datafusion_engine.runtime import diagnostics_arrow_ingest_hook
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan, stream_ibis_plan
from ibis_engine.params_bridge import param_binding_mode, param_binding_signature
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import IbisCachePolicy
from schema_spec.policies import DataFusionWritePolicy
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    write_datafusion_delta,
    write_table_delta,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.runtime import DataFusionRuntimeProfile


def _default_plan_id(plan: IbisPlan) -> str:
    label = getattr(plan, "label", "")
    if isinstance(label, str) and label:
        return label
    return "plan"


def _resolve_prefer_reader(
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
) -> bool:
    if ctx.determinism == DeterminismTier.CANONICAL:
        return False
    return policy.prefer_streaming


def _cache_event_reporter(
    ctx: ExecutionContext,
) -> Callable[[Mapping[str, object]], None] | None:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None
    diagnostics = profile.diagnostics_sink
    if diagnostics is None:
        return None
    record_events = diagnostics.record_events

    def _record(event: Mapping[str, object]) -> None:
        record_events("ibis_cache_events_v1", [event])

    return _record


def _resolve_cache_policy(
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
    prefer_reader: bool,
    params: Mapping[IbisValue, object] | Mapping[str, object] | None,
) -> IbisCachePolicy:
    writer_strategy = policy.writer_strategy
    param_mode = param_binding_mode(params)
    param_signature = param_binding_signature(params)
    if param_mode != "none":
        return IbisCachePolicy(
            enabled=False,
            reason="params",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if writer_strategy != "arrow":
        return IbisCachePolicy(
            enabled=False,
            reason=f"writer_strategy_{writer_strategy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if prefer_reader:
        return IbisCachePolicy(
            enabled=False,
            reason="prefer_streaming",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if ctx.determinism == DeterminismTier.BEST_EFFORT:
        return IbisCachePolicy(
            enabled=False,
            reason="best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    if policy.determinism_tier == DeterminismTier.BEST_EFFORT:
        return IbisCachePolicy(
            enabled=False,
            reason="policy_best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )
    return IbisCachePolicy(
        enabled=True,
        reason="materialize",
        writer_strategy=writer_strategy,
        param_mode=param_mode,
        param_signature=param_signature,
    )


def resolve_cache_policy(
    *,
    ctx: ExecutionContext,
    policy: ExecutionSurfacePolicy,
    prefer_reader: bool,
    params: Mapping[IbisValue, object] | Mapping[str, object] | None,
) -> IbisCachePolicy:
    """Return the resolved cache policy for plan materialization.

    Returns
    -------
    IbisCachePolicy
        Cache policy for the materialization.
    """
    return _resolve_cache_policy(
        ctx=ctx,
        policy=policy,
        prefer_reader=prefer_reader,
        params=params,
    )


def resolve_prefer_reader(*, ctx: ExecutionContext, policy: ExecutionSurfacePolicy) -> bool:
    """Return the prefer_reader flag for plan execution.

    Returns
    -------
    bool
        ``True`` when plan execution should prefer streaming readers.
    """
    return _resolve_prefer_reader(ctx=ctx, policy=policy)


def build_plan_product(
    plan: IbisPlan,
    *,
    execution: IbisExecutionContext,
    policy: ExecutionSurfacePolicy,
    plan_id: str | None = None,
) -> PlanProduct:
    """Execute a plan and return a PlanProduct wrapper.

    Returns
    -------
    PlanProduct
        Plan output with schema and materialization metadata.

    Raises
    ------
    ValueError
        Raised when a reader materialization is missing the expected stream.
    """
    ctx = execution.ctx
    prefer_reader = _resolve_prefer_reader(ctx=ctx, policy=policy)
    cache_policy = _resolve_cache_policy(
        ctx=ctx,
        policy=policy,
        prefer_reader=prefer_reader,
        params=execution.params,
    )
    reporter = _cache_event_reporter(ctx)
    if reporter is not None:
        cache_policy = replace(cache_policy, reporter=reporter)
    execution = replace(execution, cache_policy=cache_policy)
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
    if prefer_reader:
        stream = stream_ibis_plan(plan, execution=execution)
        if not isinstance(stream, pa.RecordBatchReader):
            msg = "Expected RecordBatchReader for reader materialization."
            raise ValueError(msg)
        schema = stream.schema
    else:
        table = materialize_ibis_plan(plan, execution=execution)
        schema = table.schema
    return PlanProduct(
        plan_id=plan_id or _default_plan_id(plan),
        schema=schema,
        determinism_tier=ctx.determinism,
        writer_strategy=policy.writer_strategy,
        stream=stream,
        table=table,
    )


def _ast_write_policy(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    schema: pa.Schema,
) -> DataFusionWritePolicy | None:
    policy = runtime_profile.write_policy
    available = set(schema.names)
    default_partitions = tuple(
        name for name, _ in runtime_profile.ast_external_partition_cols if name in available
    )
    default_sort = tuple(
        name for name in runtime_profile.ast_external_ordering if name in available
    )
    if policy is None:
        if not default_partitions and not default_sort:
            return None
        return DataFusionWritePolicy(
            partition_by=default_partitions,
            sort_by=default_sort,
        )
    return DataFusionWritePolicy(
        partition_by=policy.partition_by or default_partitions,
        single_file_output=policy.single_file_output,
        sort_by=policy.sort_by or default_sort,
        parquet_compression=policy.parquet_compression,
        parquet_statistics_enabled=policy.parquet_statistics_enabled,
        parquet_row_group_size=policy.parquet_row_group_size,
    )


def _deregister_table(ctx: SessionContext, *, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


@dataclass(frozen=True)
class _AstWriteRecord:
    mode: str
    path: str
    file_format: str
    rows: int | None
    write_policy: DataFusionWritePolicy | None
    parquet_payload: Mapping[str, object] | None
    delta_result: DeltaWriteResult | None


def _record_ast_write(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    record: _AstWriteRecord,
) -> None:
    sink = runtime_profile.diagnostics_sink
    if sink is None:
        return
    parquet_options: dict[str, object] | None = None
    if record.parquet_payload is not None:
        options_value = record.parquet_payload.get("parquet_options")
        if isinstance(options_value, Mapping):
            parquet_options = dict(options_value)
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "ast_files_v1",
        "mode": record.mode,
        "path": record.path,
        "format": record.file_format,
        "rows": record.rows,
        "write_policy": record.write_policy.payload() if record.write_policy is not None else None,
        "parquet_options": parquet_options,
        "delta_version": record.delta_result.version if record.delta_result is not None else None,
    }
    sink.record_artifact("datafusion_ast_output_writes_v1", payload)


def _write_ast_external(
    table: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    path: str,
    file_format: str,
) -> None:
    if file_format != "parquet":
        msg = f"AST DataFusion writes only support parquet, got {file_format!r}."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    temp_name = f"__ast_write_{uuid.uuid4().hex}"
    ingest_hook = (
        diagnostics_arrow_ingest_hook(runtime_profile.diagnostics_sink)
        if runtime_profile.diagnostics_sink is not None
        else None
    )
    df = datafusion_from_arrow(
        df_ctx,
        name=temp_name,
        value=table,
        ingest_hook=ingest_hook,
    )
    policy = _ast_write_policy(runtime_profile, schema=table.schema)
    try:
        payload = datafusion_write_parquet(df, path=path, policy=policy)
    finally:
        _deregister_table(df_ctx, name=temp_name)
    rows = int(table.num_rows) if isinstance(table, pa.Table) else None
    _record_ast_write(
        runtime_profile,
        record=_AstWriteRecord(
            mode="copy",
            path=str(path),
            file_format=file_format,
            rows=rows,
            write_policy=policy,
            parquet_payload=payload,
            delta_result=None,
        ),
    )


def _write_ast_delta(
    table: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    path: str,
    storage_options: Mapping[str, str] | None,
) -> None:
    df_ctx = runtime_profile.session_context()
    temp_name = f"__ast_delta_{uuid.uuid4().hex}"
    ingest_hook = (
        diagnostics_arrow_ingest_hook(runtime_profile.diagnostics_sink)
        if runtime_profile.diagnostics_sink is not None
        else None
    )
    df = datafusion_from_arrow(
        df_ctx,
        name=temp_name,
        value=table,
        ingest_hook=ingest_hook,
    )
    delta_options = DeltaWriteOptions(mode="append")
    try:
        try:
            datafusion_result = write_datafusion_delta(
                df,
                base_dir=path,
                options=delta_options,
                storage_options=storage_options,
            )
        except (TypeError, ValueError):
            datafusion_result = write_table_delta(
                table,
                path,
                options=delta_options,
                storage_options=storage_options,
            )
    finally:
        _deregister_table(df_ctx, name=temp_name)
    rows = int(table.num_rows) if isinstance(table, pa.Table) else None
    _record_ast_write(
        runtime_profile,
        record=_AstWriteRecord(
            mode="insert",
            path=str(path),
            file_format="delta",
            rows=rows,
            write_policy=None,
            parquet_payload=None,
            delta_result=datafusion_result,
        ),
    )


def write_ast_outputs(
    name: str,
    table: TableLike,
    *,
    ctx: ExecutionContext,
) -> None:
    """Write AST outputs using DataFusion-native paths when configured.

    Parameters
    ----------
    name:
        Dataset name to write (expects ``ast_files_v1``).
    table:
        Normalized AST output table.
    ctx:
        Execution context containing the DataFusion runtime profile.
    """
    if name != "ast_files_v1":
        return
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        return
    location = runtime_profile.ast_dataset_location()
    if location is None:
        return
    if location.format == "delta":
        _write_ast_delta(
            table,
            runtime_profile=runtime_profile,
            path=str(location.path),
            storage_options=(
                dict(location.storage_options) if location.storage_options is not None else None
            ),
        )
        return
    _write_ast_external(
        table,
        runtime_profile=runtime_profile,
        path=str(location.path),
        file_format=location.format,
    )


__all__ = ["build_plan_product", "resolve_prefer_reader", "write_ast_outputs"]
