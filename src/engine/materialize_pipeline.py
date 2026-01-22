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
from datafusion_engine.bridge import (
    CopyToOptions,
    DataFusionDmlOptions,
    copy_to_path,
    copy_to_statement,
    datafusion_from_arrow,
    datafusion_to_reader,
    datafusion_write_parquet,
)
from datafusion_engine.dataset_locations import resolve_dataset_location
from datafusion_engine.runtime import (
    DataFusionRuntimeProfile,
    diagnostics_arrow_ingest_hook,
    diagnostics_dml_hook,
    statement_sql_options_for_profile,
)
from engine.plan_policy import ExecutionSurfacePolicy
from engine.plan_product import PlanProduct
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext, materialize_ibis_plan, stream_ibis_plan
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from ibis_engine.params_bridge import param_binding_mode, param_binding_signature
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import IbisCachePolicy
from schema_spec.policies import DataFusionWritePolicy
from storage.deltalake import DeltaWriteResult

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

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


def df_to_reader(df: DataFrame) -> pa.RecordBatchReader:
    """Convert a DataFusion DataFrame to a streaming RecordBatchReader.

    Prefers the __arrow_c_stream__ protocol for zero-copy streaming.

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to convert.

    Returns
    -------
    pa.RecordBatchReader
        Streaming reader for the DataFrame results.
    """
    return datafusion_to_reader(df)


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
    sink.record_artifact("datafusion_extract_output_writes_v1", payload)


def _copy_statement_overrides(
    policy: DataFusionWritePolicy | None,
    file_format: str,
) -> dict[str, object] | None:
    if policy is None or file_format != "parquet":
        return None
    options: dict[str, object] = {}
    if policy.parquet_compression is not None:
        options["compression"] = policy.parquet_compression
    if policy.parquet_statistics_enabled is not None:
        options["statistics_enabled"] = policy.parquet_statistics_enabled
    if policy.parquet_row_group_size is not None:
        options["max_row_group_size"] = int(policy.parquet_row_group_size)
    return options or None


def _copy_options_payload(
    *,
    file_format: str,
    partition_by: Sequence[str],
    statement_overrides: Mapping[str, object] | None,
) -> dict[str, object] | None:
    payload: dict[str, object] = {
        "file_format": file_format,
        "partition_by": list(partition_by) if partition_by else None,
    }
    if statement_overrides is not None:
        payload["statement_overrides"] = dict(statement_overrides)
    return payload


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _copy_select_sql(
    table_name: str,
    *,
    sort_by: Sequence[str],
    schema: pa.Schema,
) -> str:
    available = set(schema.names)
    order_by = [name for name in sort_by if name in available]
    base = f"SELECT * FROM {_sql_identifier(table_name)}"
    if not order_by:
        return base
    ordering = ", ".join(_sql_identifier(name) for name in order_by)
    return f"{base} ORDER BY {ordering}"


def _copy_options(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    dataset: str,
    policy: DataFusionWritePolicy | None,
    file_format: str,
    schema: pa.Schema,
) -> CopyToOptions:
    partition_by: tuple[str, ...] = ()
    if policy is not None:
        available = set(schema.names)
        partition_by = tuple(name for name in policy.partition_by if name in available)
    statement_overrides = _copy_statement_overrides(policy, file_format)
    record_hook = None
    if runtime_profile.diagnostics_sink is not None:
        base_hook = diagnostics_dml_hook(runtime_profile.diagnostics_sink)

        def _hook(payload: Mapping[str, object]) -> None:
            merged = dict(payload)
            merged["dataset"] = dataset
            merged["statement_type"] = "COPY"
            merged["file_format"] = file_format
            merged["partition_by"] = list(partition_by) if partition_by else None
            merged["copy_options"] = (
                dict(statement_overrides) if statement_overrides is not None else None
            )
            base_hook(merged)

        record_hook = _hook
    return CopyToOptions(
        file_format=file_format,
        partition_by=partition_by,
        statement_overrides=statement_overrides,
        allow_file_output=True,
        dml=DataFusionDmlOptions(
            sql_options=statement_sql_options_for_profile(runtime_profile),
            record_hook=record_hook,
        ),
    )


def _ibis_execution_from_ctx(ctx: ExecutionContext) -> IbisExecutionContext:
    runtime_profile = ctx.runtime
    datafusion_profile = runtime_profile.datafusion
    if datafusion_profile is None:
        datafusion_profile = DataFusionRuntimeProfile()
    backend = build_backend(
        IbisBackendConfig(
            datafusion_profile=datafusion_profile,
            fuse_selects=runtime_profile.ibis_fuse_selects,
            default_limit=runtime_profile.ibis_default_limit,
            default_dialect=runtime_profile.ibis_default_dialect,
            interactive=runtime_profile.ibis_interactive,
        )
    )
    return IbisExecutionContext(ctx=ctx, ibis_backend=backend)


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
            name for name in runtime_profile.ast_external_ordering if name in available
        )
    elif dataset == "bytecode_files_v1":
        default_partition = tuple(
            name
            for name, _ in runtime_profile.bytecode_external_partition_cols
            if name in available
        )
        default_sort = tuple(
            name for name in runtime_profile.bytecode_external_ordering if name in available
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


def _deregister_table(ctx: SessionContext, *, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


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


def _write_external(
    reader: RecordBatchReaderLike,
    *,
    context: _ExternalWriteContext,
) -> None:
    location = context.location
    normalized_format = location.format.lower()
    if normalized_format not in {"parquet", "csv", "json"}:
        msg = (
            "DataFusion writes only support parquet/csv/json, "
            f"got {context.location.format!r}."
        )
        raise ValueError(msg)
    df_ctx = context.runtime_profile.session_context()
    temp_name = f"__extract_write_{uuid.uuid4().hex}"
    view_name: str | None = None
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
        if normalized_format == "parquet":
            parquet_payload = datafusion_write_parquet(
                df,
                path=str(location.path),
                policy=context.write_policy,
            )
            _record_extract_write(
                context.runtime_profile,
                record=_ExtractWriteRecord(
                    dataset=context.dataset,
                    mode="copy",
                    path=str(location.path),
                    file_format=normalized_format,
                    rows=context.rows,
                    write_policy=context.write_policy,
                    parquet_payload=parquet_payload,
                    copy_sql=None,
                    copy_options=None,
                    delta_result=None,
                ),
            )
            return
        copy_options = _copy_options(
            context.runtime_profile,
            dataset=context.dataset,
            policy=context.write_policy,
            file_format=normalized_format,
            schema=df.schema(),
        )
        view_name = f"__extract_view_{uuid.uuid4().hex}"
        df_ctx.register_table(view_name, df)
        select_sql = _copy_select_sql(
            view_name,
            sort_by=context.write_policy.sort_by if context.write_policy else (),
            schema=df.schema(),
        )
        copy_payload = _copy_options_payload(
            file_format=normalized_format,
            partition_by=copy_options.partition_by,
            statement_overrides=copy_options.statement_overrides,
        )
        copy_sql = copy_to_statement(
            select_sql,
            path=str(location.path),
            options=copy_options,
        )
        copy_to_path(
            df_ctx,
            sql=select_sql,
            path=str(location.path),
            options=copy_options,
        ).collect()
        _record_extract_write(
            context.runtime_profile,
            record=_ExtractWriteRecord(
                dataset=context.dataset,
                mode="copy",
                path=str(location.path),
                file_format=normalized_format,
                rows=context.rows,
                write_policy=context.write_policy,
                parquet_payload=None,
                copy_sql=copy_sql,
                copy_options=copy_payload,
                delta_result=None,
            ),
        )
    finally:
        _deregister_table(df_ctx, name=temp_name)
        if view_name is not None:
            _deregister_table(df_ctx, name=view_name)


def _write_delta(
    reader: RecordBatchReaderLike,
    *,
    context: _DeltaWriteContext,
) -> None:
    datafusion_result = write_ibis_dataset_delta(
        reader,
        str(context.location.path),
        options=IbisDatasetWriteOptions(
            execution=context.execution,
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="append",
                storage_options=context.location.storage_options,
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
        return
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


__all__ = [
    "build_plan_product",
    "df_to_reader",
    "resolve_cache_policy",
    "resolve_prefer_reader",
    "write_extract_outputs",
]
