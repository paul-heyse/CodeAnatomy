"""Unified materialization and output writing helpers."""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReader, RecordBatchReaderLike, TableLike
from cache.diskcache_factory import DiskCacheKind, cache_for_kind, diskcache_stats_snapshot
from datafusion_engine.bridge import datafusion_from_arrow
from datafusion_engine.dataset_locations import resolve_dataset_location
from datafusion_engine.diagnostics import record_artifact, record_events, recorder_for_profile
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.runtime import (
    DataFusionRuntimeProfile,
    diagnostics_arrow_ingest_hook,
    statement_sql_options_for_profile,
)
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.write_pipeline import (
    ParquetWritePolicy,
    WriteFormat,
    WriteMethod,
    WriteMode,
    WritePipeline,
    WriteRequest,
    WriteResult,
    parquet_policy_from_datafusion,
)
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from ibis_engine.execution import execute_ibis_plan
from ibis_engine.execution_factory import ibis_execution_from_ctx
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from ibis_engine.params_bridge import param_binding_mode, param_binding_signature
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import resolve_delta_log_storage_options
from ibis_engine.runner import IbisCachePolicy
from schema_spec.policies import DataFusionWritePolicy
from sqlglot_tools.compat import exp
from storage.deltalake import DeltaWriteResult

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from ibis_engine.execution import IbisExecutionContext
    from ibis_engine.registry import DatasetLocation


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
    result = execute_ibis_plan(
        plan,
        execution=execution,
        streaming=prefer_reader,
    )
    if prefer_reader:
        stream = result.require_reader()
        if not isinstance(stream, pa.RecordBatchReader):
            msg = "Expected RecordBatchReader for reader materialization."
            raise ValueError(msg)
        schema = stream.schema
    else:
        table = result.require_table()
        schema = table.schema
    return PlanProduct(
        plan_id=plan_id or _default_plan_id(plan),
        schema=schema,
        determinism_tier=ctx.determinism,
        writer_strategy=policy.writer_strategy,
        stream=stream,
        table=table,
        execution_result=result,
    )


@dataclass(frozen=True)
class _ExtractWriteRecord:
    dataset: str
    mode: str
    path: str
    file_format: str
    rows: int | None
    write_policy: DataFusionWritePolicy | None
    parquet_payload: Mapping[str, object] | None
    copy_sql: str | None
    copy_options: Mapping[str, object] | None
    delta_result: DeltaWriteResult | None


def _record_extract_write(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    record: _ExtractWriteRecord,
) -> None:
    parquet_options: dict[str, object] | None = None
    if record.parquet_payload is not None:
        options_value = record.parquet_payload.get("parquet_options")
        if isinstance(options_value, Mapping):
            parquet_options = dict(options_value)
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": record.dataset,
        "mode": record.mode,
        "path": record.path,
        "format": record.file_format,
        "rows": record.rows,
        "write_policy": record.write_policy.payload() if record.write_policy is not None else None,
        "parquet_options": parquet_options,
        "copy_sql": record.copy_sql,
        "copy_options": dict(record.copy_options) if record.copy_options is not None else None,
        "delta_version": record.delta_result.version if record.delta_result is not None else None,
    }
    record_artifact(runtime_profile, "datafusion_extract_output_writes_v1", payload)


def _record_diskcache_stats(runtime_profile: DataFusionRuntimeProfile) -> None:
    profile = runtime_profile.diskcache_profile
    if profile is None:
        return
    events: list[dict[str, object]] = []
    for kind in ("plan", "extract", "schema", "repo_scan", "runtime", "coordination"):
        cache = cache_for_kind(profile, cast("DiskCacheKind", kind))
        settings = profile.settings_for(cast("DiskCacheKind", kind))
        payload = diskcache_stats_snapshot(cache)
        payload.update(
            {
                "kind": kind,
                "profile_key": runtime_profile.context_cache_key(),
                "size_limit_bytes": settings.size_limit_bytes,
                "eviction_policy": settings.eviction_policy,
                "cull_limit": settings.cull_limit,
                "shards": settings.shards,
                "statistics": settings.statistics,
                "tag_index": settings.tag_index,
                "disk_min_file_size": settings.disk_min_file_size,
                "sqlite_journal_mode": settings.sqlite_journal_mode,
                "sqlite_mmap_size": settings.sqlite_mmap_size,
                "sqlite_synchronous": settings.sqlite_synchronous,
            }
        )
        events.append(payload)
    if events:
        record_events(runtime_profile, "diskcache_stats_v1", events)


def _write_pipeline_profile() -> SQLPolicyProfile:
    from sqlglot_tools.optimizer import register_datafusion_dialect, resolve_sqlglot_policy

    policy = resolve_sqlglot_policy(name="datafusion_dml")
    register_datafusion_dialect(policy.write_dialect)
    return SQLPolicyProfile(
        read_dialect=policy.read_dialect,
        write_dialect=policy.write_dialect,
    )


def _write_format(value: str) -> WriteFormat:
    normalized = value.strip().lower()
    mapping = {
        "parquet": WriteFormat.PARQUET,
        "csv": WriteFormat.CSV,
        "json": WriteFormat.JSON,
    }
    resolved = mapping.get(normalized)
    if resolved is None:
        msg = f"DataFusion writes only support parquet/csv/json, got {value!r}."
        raise ValueError(msg)
    return resolved


def _write_partition_by(
    policy: DataFusionWritePolicy | None,
    *,
    schema: pa.Schema,
) -> tuple[str, ...]:
    if policy is None or not policy.partition_by:
        return ()
    available = set(schema.names)
    return tuple(name for name in policy.partition_by if name in available)


def _write_sort_by(
    policy: DataFusionWritePolicy | None,
    *,
    schema: pa.Schema,
) -> tuple[str, ...]:
    if policy is None or not policy.sort_by:
        return ()
    available = set(schema.names)
    return tuple(name for name in policy.sort_by if name in available)


def _write_select_expr(
    table_name: str,
    *,
    sort_by: Sequence[str],
    schema: pa.Schema,
) -> exp.Expression:
    available = set(schema.names)
    order_by = [name for name in sort_by if name in available]
    query = exp.select("*").from_(exp.table_(table_name))
    if order_by:
        order_exprs = [exp.Ordered(this=exp.column(name)) for name in order_by]
        query.set("order", exp.Order(expressions=order_exprs))
    return query


def _parquet_payload(
    *,
    path: str,
    policy: DataFusionWritePolicy | None,
    parquet_policy: ParquetWritePolicy | None,
) -> dict[str, object] | None:
    if policy is None and parquet_policy is None:
        return None
    payload: dict[str, object] = {
        "path": path,
        "write_policy": policy.payload() if policy is not None else None,
        "parquet_options": (
            parquet_policy.to_copy_options() if parquet_policy is not None else None
        ),
    }
    return payload


def _ibis_execution_from_ctx(ctx: ExecutionContext) -> IbisExecutionContext:
    if ctx.runtime.datafusion is None:
        runtime_profile = ctx.runtime.with_datafusion(DataFusionRuntimeProfile())
        ctx = ExecutionContext(runtime=runtime_profile)
    return ibis_execution_from_ctx(ctx)


def _write_policy_for_dataset(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    dataset: str,
    schema: pa.Schema,
) -> DataFusionWritePolicy | None:
    available = set(schema.names)
    default_partition: tuple[str, ...]
    default_sort: tuple[str, ...]
    if dataset == "ast_files_v1":
        default_partition = tuple(
            name for name, _ in runtime_profile.ast_external_partition_cols if name in available
        )
        default_sort = tuple(
            name for name, _ in runtime_profile.ast_external_ordering if name in available
        )
    elif dataset == "bytecode_files_v1":
        default_partition = tuple(
            name
            for name, _ in runtime_profile.bytecode_external_partition_cols
            if name in available
        )
        default_sort = tuple(
            name for name, _ in runtime_profile.bytecode_external_ordering if name in available
        )
    else:
        default_partition = tuple(name for name in ("repo",) if name in available)
        default_sort = tuple(name for name in ("path", "file_id") if name in available)
    policy = runtime_profile.write_policy
    if policy is None:
        if not default_partition and not default_sort:
            return None
        return DataFusionWritePolicy(
            partition_by=default_partition,
            sort_by=default_sort,
        )
    return DataFusionWritePolicy(
        partition_by=policy.partition_by or default_partition,
        single_file_output=policy.single_file_output,
        sort_by=policy.sort_by or default_sort,
        parquet_compression=policy.parquet_compression,
        parquet_statistics_enabled=policy.parquet_statistics_enabled,
        parquet_row_group_size=policy.parquet_row_group_size,
        parquet_bloom_filter_on_write=policy.parquet_bloom_filter_on_write,
        parquet_dictionary_enabled=policy.parquet_dictionary_enabled,
        parquet_encoding=policy.parquet_encoding,
        parquet_skip_arrow_metadata=policy.parquet_skip_arrow_metadata,
        parquet_column_options=policy.parquet_column_options,
    )


def _coerce_reader(
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
) -> tuple[RecordBatchReaderLike | None, int | None]:
    if isinstance(data, pa.Table):
        table = cast("pa.Table", data)
        return (
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            int(table.num_rows),
        )
    if isinstance(data, RecordBatchReader):
        return cast("RecordBatchReaderLike", data), None
    if isinstance(data, Sequence):
        batches = list(cast("Sequence[pa.RecordBatch]", data))
        if not batches:
            return None, 0
        rows = sum(batch.num_rows for batch in batches)
        reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
        return reader, rows
    iterator = iter(cast("Iterable[pa.RecordBatch]", data))
    try:
        first = next(iterator)
    except StopIteration:
        return None, 0

    def _iter_batches() -> Iterable[pa.RecordBatch]:
        yield first
        yield from iterator

    reader = pa.RecordBatchReader.from_batches(first.schema, _iter_batches())
    return reader, None


@dataclass(frozen=True)
class _ExternalWriteContext:
    dataset: str
    runtime_profile: DataFusionRuntimeProfile
    location: DatasetLocation
    write_policy: DataFusionWritePolicy | None
    rows: int | None


@dataclass(frozen=True)
class _DeltaWriteContext:
    dataset: str
    runtime_profile: DataFusionRuntimeProfile
    location: DatasetLocation
    rows: int | None
    execution: IbisExecutionContext


@dataclass(frozen=True)
class _ExternalCopyDiagnosticsContext:
    runtime_profile: DataFusionRuntimeProfile
    dataset: str
    result: WriteResult
    parquet_policy: ParquetWritePolicy | None
    rows: int | None
    write_policy: DataFusionWritePolicy | None


def _build_external_write_request(
    *,
    context: _ExternalWriteContext,
    temp_name: str,
    df: DataFrame,
    write_format: WriteFormat,
) -> tuple[WriteRequest, ParquetWritePolicy | None]:
    schema = cast("pa.Schema", df.schema())
    sort_by = _write_sort_by(context.write_policy, schema=schema)
    select_expr = _write_select_expr(temp_name, sort_by=sort_by, schema=schema)
    partition_by = _write_partition_by(context.write_policy, schema=schema)
    parquet_policy = parquet_policy_from_datafusion(context.write_policy)
    request = WriteRequest(
        source=select_expr,
        destination=str(context.location.path),
        format=write_format,
        mode=WriteMode.OVERWRITE,
        partition_by=partition_by,
        parquet_policy=parquet_policy,
        single_file_output=(
            context.write_policy.single_file_output if context.write_policy is not None else None
        ),
    )
    return request, parquet_policy


def _build_external_write_pipeline(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    operation_id: str,
) -> tuple[WritePipeline, SQLPolicyProfile]:
    profile = _write_pipeline_profile()
    pipeline = WritePipeline(
        runtime_profile.session_context(),
        profile,
        sql_options=statement_sql_options_for_profile(runtime_profile),
        recorder=recorder_for_profile(runtime_profile, operation_id=operation_id),
    )
    return pipeline, profile


def _emit_external_copy_diagnostics(context: _ExternalCopyDiagnosticsContext) -> None:
    request = context.result.request
    copy_sql = context.result.sql if context.result.method == WriteMethod.COPY else None
    parquet_payload = (
        _parquet_payload(
            path=str(request.destination),
            policy=context.write_policy,
            parquet_policy=context.parquet_policy,
        )
        if request.format == WriteFormat.PARQUET
        else None
    )
    options: dict[str, object] = {"format": request.format.name}
    if request.format_options:
        options.update(request.format_options)
    if context.parquet_policy is not None:
        options.update(context.parquet_policy.to_copy_options())
    copy_payload: Mapping[str, object] | None = options or None
    _record_extract_write(
        context.runtime_profile,
        record=_ExtractWriteRecord(
            dataset=context.dataset,
            mode="copy",
            path=str(request.destination),
            file_format=request.format.name.lower(),
            rows=context.rows,
            write_policy=context.write_policy,
            parquet_payload=parquet_payload,
            copy_sql=copy_sql,
            copy_options=copy_payload,
            delta_result=None,
        ),
    )


def _write_external(
    reader: RecordBatchReaderLike,
    *,
    context: _ExternalWriteContext,
) -> None:
    location = context.location
    write_format = _write_format(location.format)
    df_ctx = context.runtime_profile.session_context()
    temp_name = f"__extract_write_{uuid.uuid4().hex}"
    ingest_hook = (
        diagnostics_arrow_ingest_hook(context.runtime_profile.diagnostics_sink)
        if context.runtime_profile.diagnostics_sink is not None
        else None
    )
    df = datafusion_from_arrow(
        df_ctx,
        name=temp_name,
        value=reader,
        ingest_hook=ingest_hook,
    )
    try:
        request, parquet_policy = _build_external_write_request(
            context=context,
            temp_name=temp_name,
            df=df,
            write_format=write_format,
        )
        pipeline, _ = _build_external_write_pipeline(
            context.runtime_profile,
            operation_id=f"extract_write:{context.dataset}",
        )
        result = pipeline.write(request, prefer_streaming=True)
        _emit_external_copy_diagnostics(
            _ExternalCopyDiagnosticsContext(
                runtime_profile=context.runtime_profile,
                dataset=context.dataset,
                result=result,
                parquet_policy=parquet_policy,
                rows=context.rows,
                write_policy=context.write_policy,
            )
        )
    finally:
        adapter = DataFusionIOAdapter(ctx=df_ctx, profile=context.runtime_profile)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(temp_name)


def _write_delta(
    reader: RecordBatchReaderLike,
    *,
    context: _DeltaWriteContext,
) -> None:
    log_storage_options = resolve_delta_log_storage_options(context.location)
    datafusion_result = write_ibis_dataset_delta(
        reader,
        str(context.location.path),
        options=IbisDatasetWriteOptions(
            execution=context.execution,
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="append",
                storage_options=context.location.storage_options,
                log_storage_options=log_storage_options,
            ),
        ),
    )
    _record_extract_write(
        context.runtime_profile,
        record=_ExtractWriteRecord(
            dataset=context.dataset,
            mode="insert",
            path=str(context.location.path),
            file_format="delta",
            rows=context.rows,
            write_policy=None,
            parquet_payload=None,
            copy_sql=None,
            copy_options=None,
            delta_result=datafusion_result,
        ),
    )


def write_extract_outputs(
    name: str,
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    ctx: ExecutionContext,
) -> None:
    """Write extract outputs using DataFusion-native paths when configured.

    Raises
    ------
    ValueError
        Raised when the DataFusion runtime profile is missing, a dataset location is not
        registered, or the output yields no rows.
    """
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        msg = "DataFusion runtime profile is required for extract outputs."
        raise ValueError(msg)
    runtime_profile.record_schema_snapshots()
    location = resolve_dataset_location(name, runtime_profile=runtime_profile)
    if location is None:
        msg = f"No dataset location registered for extract output {name!r}."
        raise ValueError(msg)
    reader, rows = _coerce_reader(data)
    if reader is None:
        msg = f"Extract output {name!r} yielded no rows."
        raise ValueError(msg)
    schema = cast("pa.Schema", reader.schema)
    policy = _write_policy_for_dataset(runtime_profile, dataset=name, schema=schema)
    if location.format.lower() == "delta":
        execution = _ibis_execution_from_ctx(ctx)
        _write_delta(
            reader,
            context=_DeltaWriteContext(
                dataset=name,
                runtime_profile=runtime_profile,
                location=location,
                rows=rows,
                execution=execution,
            ),
        )
    else:
        _write_external(
            reader,
            context=_ExternalWriteContext(
                dataset=name,
                runtime_profile=runtime_profile,
                location=location,
                write_policy=policy,
                rows=rows,
            ),
        )
    _record_diskcache_stats(runtime_profile)


__all__ = [
    "build_plan_product",
    "resolve_cache_policy",
    "resolve_prefer_reader",
    "write_extract_outputs",
]
