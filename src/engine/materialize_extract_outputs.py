"""Materialize extract outputs to configured DataFusion dataset locations."""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReader, RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import (
    CopyToOptions,
    DataFusionDmlOptions,
    copy_to_path,
    copy_to_statement,
    datafusion_from_arrow,
    datafusion_write_parquet,
)
from datafusion_engine.runtime import (
    diagnostics_arrow_ingest_hook,
    diagnostics_dml_hook,
    statement_sql_options_for_profile,
)
from schema_spec.policies import DataFusionWritePolicy
from storage.deltalake import DeltaWriteOptions, DeltaWriteResult, write_datafusion_delta

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation


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
    batches = list(cast("Iterable[pa.RecordBatch]", data))
    if not batches:
        return None, 0
    rows = sum(batch.num_rows for batch in batches)
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    return reader, rows


@dataclass(frozen=True)
class _ExternalWriteContext:
    dataset: str
    runtime_profile: DataFusionRuntimeProfile
    location: DatasetLocation
    write_policy: DataFusionWritePolicy | None
    rows: int | None


def _write_external(
    reader: RecordBatchReaderLike,
    *,
    context: _ExternalWriteContext,
) -> None:
    normalized_format = context.location.format.lower()
    if normalized_format not in {"parquet", "csv", "json"}:
        msg = (
            "DataFusion writes only support parquet/csv/json, "
            f"got {context.location.format!r}."
        )
        raise ValueError(msg)
    df_ctx = context.runtime_profile.session_context()
    temp_name = f"__extract_write_{uuid.uuid4().hex}"
    ingest_hook = (
        diagnostics_arrow_ingest_hook(context.runtime_profile.diagnostics_sink)
        if context.runtime_profile.diagnostics_sink is not None
        else None
    )
    schema = cast("pa.Schema", reader.schema)
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
                path=str(context.location.path),
                policy=context.write_policy,
            )
            _record_extract_write(
                context.runtime_profile,
                record=_ExtractWriteRecord(
                    dataset=context.dataset,
                    mode="write",
                    path=str(context.location.path),
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
            schema=schema,
        )
        select_sql = _copy_select_sql(
            temp_name,
            sort_by=context.write_policy.sort_by if context.write_policy is not None else (),
            schema=schema,
        )
        copy_sql: str | None = None
        copy_sql = copy_to_statement(
            select_sql,
            path=str(context.location.path),
            options=copy_options,
        )
        copy_to_path(
            df_ctx,
            sql=select_sql,
            path=str(context.location.path),
            options=copy_options,
        ).collect()
        _record_extract_write(
            context.runtime_profile,
            record=_ExtractWriteRecord(
                dataset=context.dataset,
                mode="copy",
                path=str(context.location.path),
                file_format=normalized_format,
                rows=context.rows,
                write_policy=context.write_policy,
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
    finally:
        _deregister_table(df_ctx, name=temp_name)


def _write_delta(
    reader: RecordBatchReaderLike,
    *,
    dataset: str,
    runtime_profile: DataFusionRuntimeProfile,
    location: DatasetLocation,
    rows: int | None,
) -> None:
    df_ctx = runtime_profile.session_context()
    temp_name = f"__extract_delta_{uuid.uuid4().hex}"
    ingest_hook = (
        diagnostics_arrow_ingest_hook(runtime_profile.diagnostics_sink)
        if runtime_profile.diagnostics_sink is not None
        else None
    )
    df = datafusion_from_arrow(
        df_ctx,
        name=temp_name,
        value=reader,
        ingest_hook=ingest_hook,
    )
    try:
        datafusion_result = write_datafusion_delta(
            df,
            base_dir=str(location.path),
            options=DeltaWriteOptions(mode="append"),
            storage_options=(
                dict(location.storage_options) if location.storage_options is not None else None
            ),
            runtime_profile=runtime_profile,
        )
    finally:
        _deregister_table(df_ctx, name=temp_name)
    _record_extract_write(
        runtime_profile,
        record=_ExtractWriteRecord(
            dataset=dataset,
            mode="insert",
            path=str(location.path),
            file_format="delta",
            rows=rows,
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
    """Write extract outputs using DataFusion-native paths when configured."""
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        return
    location = runtime_profile.extract_dataset_location(name)
    if location is None:
        return
    reader, rows = _coerce_reader(data)
    if reader is None:
        return
    schema = cast("pa.Schema", reader.schema)
    write_policy = _write_policy_for_dataset(runtime_profile, dataset=name, schema=schema)
    if location.format == "delta":
        _write_delta(
            reader,
            dataset=name,
            runtime_profile=runtime_profile,
            location=location,
            rows=rows,
        )
        return
    _write_external(
        reader,
        context=_ExternalWriteContext(
            dataset=name,
            runtime_profile=runtime_profile,
            location=location,
            write_policy=write_policy,
            rows=rows,
        ),
    )


__all__ = ["write_extract_outputs"]
