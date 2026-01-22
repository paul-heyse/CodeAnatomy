"""Materialization helpers for plan outputs."""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion.dataframe import DataFrame
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.io.parquet import ParquetWriteOptions, write_table_parquet
from datafusion_engine.bridge import (
    CopyToOptions,
    DataFusionDmlOptions,
    copy_to_path,
    copy_to_statement,
    datafusion_from_arrow,
    datafusion_to_reader,
)
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
    copy_sql: str | None
    copy_options: Mapping[str, object] | None
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
        "copy_sql": record.copy_sql,
        "copy_options": dict(record.copy_options) if record.copy_options is not None else None,
        "delta_version": record.delta_result.version if record.delta_result is not None else None,
    }
    sink.record_artifact("datafusion_ast_output_writes_v1", payload)


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
) -> dict[str, object]:
    payload: dict[str, object] = {"file_format": file_format}
    if partition_by:
        payload["partition_by"] = list(partition_by)
    payload["options"] = dict(statement_overrides) if statement_overrides else None
    return payload


def _ast_copy_options(
    *,
    runtime_profile: DataFusionRuntimeProfile,
    policy: DataFusionWritePolicy | None,
    file_format: str,
    schema: pa.Schema,
) -> CopyToOptions:
    from datafusion_engine.runtime import (
        diagnostics_dml_hook,
        statement_sql_options_for_profile,
    )

    partition_by = ()
    if policy is not None:
        available = set(schema.names)
        partition_by = tuple(name for name in policy.partition_by if name in available)
    statement_overrides = _copy_statement_overrides(policy, file_format)
    record_hook = None
    if runtime_profile.diagnostics_sink is not None:
        base_hook = diagnostics_dml_hook(runtime_profile.diagnostics_sink)

        def _hook(payload: Mapping[str, object]) -> None:
            merged = dict(payload)
            merged["dataset"] = "ast_files_v1"
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


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _write_ast_external(
    table: TableLike,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    path: str,
    file_format: str,
) -> None:
    from datafusion_engine.runtime import diagnostics_arrow_ingest_hook

    normalized_format = file_format.lower()
    if normalized_format not in {"parquet", "csv", "json"}:
        msg = f"AST DataFusion writes only support parquet/csv/json, got {file_format!r}."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    temp_name = f"__ast_write_{uuid.uuid4().hex}"
    ingest_hook = (
        diagnostics_arrow_ingest_hook(runtime_profile.diagnostics_sink)
        if runtime_profile.diagnostics_sink is not None
        else None
    )
    _ = datafusion_from_arrow(
        df_ctx,
        name=temp_name,
        value=table,
        ingest_hook=ingest_hook,
    )
    policy = _ast_write_policy(runtime_profile, schema=table.schema)
    copy_options = _ast_copy_options(
        runtime_profile=runtime_profile,
        policy=policy,
        file_format=normalized_format,
        schema=table.schema,
    )
    select_sql = _copy_select_sql(
        temp_name,
        sort_by=policy.sort_by if policy is not None else (),
        schema=table.schema,
    )
    copy_sql: str | None = None
    try:
        copy_sql = copy_to_statement(select_sql, path=str(path), options=copy_options)
        copy_to_path(df_ctx, sql=select_sql, path=str(path), options=copy_options).collect()
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
            parquet_payload=None,
            copy_sql=copy_sql,
            copy_options=_copy_options_payload(
                file_format=normalized_format,
                partition_by=copy_options.partition_by,
                statement_overrides=copy_options.statement_overrides,
            ),
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
    from datafusion_engine.runtime import diagnostics_arrow_ingest_hook

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
        datafusion_result = write_datafusion_delta(
            df,
            base_dir=path,
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
            copy_sql=None,
            copy_options=None,
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


def write_parquet_stream(
    reader: pa.RecordBatchReader,
    *,
    path: str,
    options: ParquetWriteOptions | None = None,
) -> str:
    """Write a RecordBatchReader to a Parquet file using streaming.

    Parameters
    ----------
    reader : pa.RecordBatchReader
        Streaming reader to write.
    path : str
        Output path for the Parquet file.
    options : ParquetWriteOptions | None
        Optional Parquet write options.

    Returns
    -------
    str
        Path to the written Parquet file.
    """
    return write_table_parquet(reader, path, opts=options, overwrite=True)


__all__ = [
    "build_plan_product",
    "df_to_reader",
    "resolve_prefer_reader",
    "write_ast_outputs",
    "write_parquet_stream",
]
