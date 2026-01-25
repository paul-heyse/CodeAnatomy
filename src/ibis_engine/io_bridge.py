"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Scalar
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.interop import (
    RecordBatchReaderLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.core.streaming import to_reader
from core_types import PathLike
from datafusion_engine.bridge import (
    DeltaInsertOptions,
    DeltaInsertResult,
    datafusion_from_arrow,
    datafusion_insert_delta,
    datafusion_insert_from_dataframe,
    datafusion_to_table,
    datafusion_view_sql,
    ibis_plan_to_datafusion,
    ibis_to_datafusion,
)
from datafusion_engine.diagnostics import recorder_for_profile
from datafusion_engine.io_adapter import DataFusionIOAdapter
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.table_provider_metadata import (
    all_table_provider_metadata,
    table_provider_metadata,
)
from datafusion_engine.write_pipeline import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)
from engine.plan_policy import WriterStrategy
from engine.plan_product import PlanProduct
from ibis_engine.execution import (
    IbisExecutionContext,
    materialize_ibis_plan,
    stream_ibis_plan,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.registry import resolve_delta_log_storage_options
from ibis_engine.runner import async_stream_plan
from ibis_engine.sources import (
    DatabaseHint,
    IbisDeltaWriteOptions,
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    record_namespace_action,
    resolve_database_hint,
    source_to_ibis,
    write_delta_ibis,
)
from sqlglot_tools.bridge import IbisCompilerBackend
from sqlglot_tools.compat import exp
from storage.deltalake import (
    DeltaDataCheckRequest,
    DeltaWriteResult,
    StorageOptions,
    delta_cdf_enabled,
    delta_data_checker,
    delta_table_features,
    delta_table_version,
)
from storage.deltalake.config import (
    DeltaSchemaPolicy,
    DeltaWritePolicy,
    delta_schema_configuration,
    delta_write_configuration,
)

type IbisWriteInput = TableLike | RecordBatchReaderLike | IbisPlan | IbisTable | PlanProduct
type DeltaInsertMode = Literal["append", "overwrite"]

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation
    from obs.datafusion_runs import DataFusionRun


class TableMaterializeBackend(Protocol):
    """Protocol for Ibis backends supporting table materialization."""

    def create_table(
        self,
        name: str,
        *,
        schema: ibis.Schema,
        database: str | None = None,
        overwrite: bool = False,
    ) -> None:
        """Create a table in the backend."""
        ...

    def insert(
        self,
        name: str,
        obj: IbisTable,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> None:
        """Insert data into a backend table."""
        ...


@dataclass(frozen=True)
class IbisMaterializeOptions:
    """Options for materializing Ibis expressions to backend tables."""

    backend: BaseBackend
    name: str
    overwrite: bool = True
    database: DatabaseHint | None = None
    namespace_recorder: Callable[[Mapping[str, object]], None] | None = None


@dataclass(frozen=True)
class IbisDatasetWriteOptions:
    """Options for writing a single dataset from Ibis inputs."""

    batch_size: int | None = None
    prefer_reader: bool = True
    use_async_streaming: bool = False
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None
    delta_reporter: Callable[[DeltaWriteResult], None] | None = None
    delta_options: IbisDeltaWriteOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None
    delta_log_storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class IbisNamedDatasetWriteOptions:
    """Options for writing named datasets from Ibis inputs."""

    batch_size: int | None = None
    prefer_reader: bool = True
    use_async_streaming: bool = False
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None
    delta_reporter: Callable[[DeltaWriteResult], None] | None = None
    delta_options: IbisDeltaWriteOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None
    delta_log_storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class IbisParquetWriteOptions:
    """Options for writing Ibis expressions to parquet directories."""

    execution: IbisExecutionContext
    overwrite: bool = True
    partition_by: Sequence[str] | None = None
    dataset_options: Mapping[str, object] | None = None


@dataclass(frozen=True)
class IbisCopyWriteOptions:
    """Options for writing Ibis expressions via DataFusion COPY."""

    execution: IbisExecutionContext
    file_format: str
    overwrite: bool = True
    partition_by: Sequence[str] | None = None
    statement_overrides: Mapping[str, object] | None = None
    record_hook: Callable[[Mapping[str, object]], None] | None = None


@dataclass(frozen=True)
class IbisCopyWriteResult:
    """Summary of a DataFusion COPY write."""

    path: str
    sql: str
    file_format: str
    partition_by: tuple[str, ...]
    statement_overrides: Mapping[str, object] | None


@dataclass(frozen=True)
class DeltaCommitReservation:
    """Reserved commit metadata for Delta writes."""

    options: IbisDeltaWriteOptions
    run: DataFusionRun | None
    key: str


@dataclass(frozen=True)
class DeltaWriteContext:
    """Shared inputs for Delta write operations."""

    execution: IbisExecutionContext
    ctx: SessionContext
    backend: BaseBackend
    runtime_profile: DataFusionRuntimeProfile
    delta_options: IbisDeltaWriteOptions
    batch_size: int | None
    reporter: Callable[[DeltaWriteResult], None] | None


@dataclass(frozen=True)
class DeltaInsertRequest:
    """Inputs required for a DataFusion-backed Delta insert."""

    ctx: SessionContext
    table_name: str
    value: IbisWriteInput
    mode: DeltaInsertMode
    backend: IbisCompilerBackend
    batch_size: int | None
    runtime_profile: DataFusionRuntimeProfile | None
    constraints: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeltaDataCheckContext:
    """Inputs required to run DeltaDataChecker."""

    ctx: SessionContext
    runtime_profile: DataFusionRuntimeProfile | None
    table_name: str
    table: TableLike
    constraints: Sequence[str]
    operation: str
    location: DatasetLocation | None = None


def _resolved_write_batch_size(options: IbisDatasetWriteOptions) -> int | None:
    if options.batch_size is not None:
        return options.batch_size
    if options.execution is None:
        return None
    return options.execution.batch_size


def _resolved_named_write_batch_size(options: IbisNamedDatasetWriteOptions) -> int | None:
    if options.batch_size is not None:
        return options.batch_size
    if options.execution is None:
        return None
    return options.execution.batch_size


def ibis_plan_to_reader(
    plan: IbisPlan,
    *,
    batch_size: int | None = None,
    execution: IbisExecutionContext | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        Reader yielding batches from the plan.
    """
    if execution is not None:
        if batch_size is not None and execution.batch_size != batch_size:
            execution = replace(execution, batch_size=batch_size)
        return stream_ibis_plan(plan, execution=execution)
    if batch_size is None:
        return to_reader(plan)
    return plan.to_reader(batch_size=batch_size)


def ibis_table_to_reader(
    table: IbisTable,
    *,
    batch_size: int | None = None,
) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis table expression.

    Returns
    -------
    RecordBatchReaderLike
        Reader yielding batches from the table.
    """
    if batch_size is None:
        return to_reader(table)
    return table.to_pyarrow_batches(chunk_size=batch_size)


def ibis_to_table(
    value: IbisPlan | IbisTable,
    *,
    execution: IbisExecutionContext | None = None,
) -> TableLike:
    """Materialize an Ibis plan or table to an Arrow table.

    Returns
    -------
    TableLike
        Materialized Arrow table.
    """
    if isinstance(value, IbisPlan):
        if execution is not None:
            return materialize_ibis_plan(value, execution=execution)
        return value.to_table()
    return value.to_pyarrow()


def materialize_table(
    expr: IbisTable,
    *,
    options: IbisMaterializeOptions,
) -> IbisTable:
    """Materialize an Ibis expression into a backend table.

    Returns
    -------
    ibis.expr.types.Table
        Table expression backed by the created table.

    Raises
    ------
    ValueError
        Raised when the target table name is empty.
    TypeError
        Raised when the backend lacks materialization methods.
    """
    database_hint, table_name = resolve_database_hint(options.name)
    if table_name is None:
        msg = "Materialization requires a non-empty table name."
        raise ValueError(msg)
    backend = cast("TableMaterializeBackend", options.backend)
    database = options.database or database_hint
    backend_database = _backend_database(database)
    create_table = getattr(backend, "create_table", None)
    if not callable(create_table):
        msg = "Ibis backend is missing create_table."
        raise TypeError(msg)
    create_table(
        table_name,
        schema=expr.schema(),
        database=backend_database,
        overwrite=options.overwrite,
    )
    record_namespace_action(
        options.namespace_recorder,
        action="create_table",
        name=table_name,
        database=database,
        overwrite=options.overwrite,
    )
    insert = getattr(backend, "insert", None)
    if not callable(insert):
        msg = "Ibis backend is missing insert."
        raise TypeError(msg)
    insert(
        table_name,
        expr,
        database=backend_database,
        overwrite=options.overwrite,
    )
    record_namespace_action(
        options.namespace_recorder,
        action="insert",
        name=table_name,
        database=database,
        overwrite=options.overwrite,
    )
    return options.backend.table(table_name, database=backend_database)


def _backend_database(database: DatabaseHint) -> str | None:
    if database is None:
        return None
    if isinstance(database, tuple):
        return database[1]
    return database


async def _collect_async_batches(
    plan: IbisPlan,
    *,
    batch_size: int | None,
    execution: IbisExecutionContext | None,
) -> list[pa.RecordBatch]:
    execution_options = execution.plan_options() if execution is not None else None
    return [
        batch
        async for batch in async_stream_plan(
            plan,
            batch_size=batch_size,
            execution=execution_options,
        )
    ]


def _async_reader_from_plan(
    plan: IbisPlan,
    *,
    batch_size: int | None,
    execution: IbisExecutionContext | None,
) -> RecordBatchReaderLike:
    try:
        batches = asyncio.run(
            _collect_async_batches(plan, batch_size=batch_size, execution=execution)
        )
    except RuntimeError as exc:
        msg = "Async streaming requires an event loop compatible with asyncio.run."
        raise RuntimeError(msg) from exc
    schema = batches[0].schema if batches else plan.expr.schema().to_pyarrow()
    return pa.RecordBatchReader.from_batches(schema, batches)


def _coerce_write_input(
    value: IbisWriteInput,
    *,
    batch_size: int | None,
    prefer_reader: bool,
    use_async_streaming: bool,
    execution: IbisExecutionContext | None,
) -> RecordBatchReaderLike | TableLike:
    result: RecordBatchReaderLike | TableLike | IbisWriteInput = value
    if isinstance(value, PlanProduct):
        result = value.value()
    elif isinstance(value, IbisPlan):
        if prefer_reader:
            if use_async_streaming:
                result = _async_reader_from_plan(
                    value,
                    batch_size=batch_size,
                    execution=execution,
                )
            else:
                result = ibis_plan_to_reader(value, batch_size=batch_size, execution=execution)
        else:
            result = ibis_to_table(value, execution=execution)
    elif isinstance(value, IbisTable):
        result = (
            ibis_table_to_reader(value, batch_size=batch_size)
            if prefer_reader
            else value.to_pyarrow()
        )
    elif _is_arrow_table_like(value) or _has_arrow_capsule(value):
        result = coerce_table_like(value)
    return cast("RecordBatchReaderLike | TableLike", result)


def _is_arrow_table_like(value: object) -> bool:
    return isinstance(value, (pa.Table, pa.RecordBatchReader))


def _has_arrow_capsule(value: object) -> bool:
    return any(
        hasattr(value, attr)
        for attr in ("__arrow_c_stream__", "__arrow_c_array__", "__dataframe__")
    )


def _resolve_writer_strategy(
    *,
    options: IbisDatasetWriteOptions,
    data: IbisWriteInput,
) -> WriterStrategy:
    if options.writer_strategy is not None:
        return options.writer_strategy
    if isinstance(data, PlanProduct):
        return data.writer_strategy
    return "datafusion"


def _merge_delta_configurations(
    *configs: Mapping[str, str | None] | None,
) -> Mapping[str, str | None] | None:
    merged: dict[str, str | None] = {}
    for config in configs:
        if not config:
            continue
        for key, value in config.items():
            if value is None:
                continue
            merged[key] = value
    return merged or None


def apply_ibis_delta_write_policies(
    options: IbisDeltaWriteOptions,
    *,
    write_policy: DeltaWritePolicy | None,
    schema_policy: DeltaSchemaPolicy | None,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> IbisDeltaWriteOptions:
    """Return Delta write options with policy overrides applied.

    Returns
    -------
    IbisDeltaWriteOptions
        Delta write options with merged policy overrides.
    """
    configs = _merge_delta_configurations(
        delta_write_configuration(write_policy),
        delta_schema_configuration(schema_policy),
        options.configuration,
    )
    schema_mode = options.schema_mode
    if schema_mode is None and schema_policy is not None:
        schema_mode = schema_policy.schema_mode
    target_file_size = options.target_file_size
    if target_file_size is None and write_policy is not None:
        target_file_size = write_policy.target_file_size
    merged_storage = options.storage_options
    if storage_options:
        merged = dict(options.storage_options or {})
        merged.update(dict(storage_options))
        merged_storage = merged
    merged_log_storage = options.log_storage_options
    if log_storage_options:
        merged = dict(options.log_storage_options or {})
        merged.update(dict(log_storage_options))
        merged_log_storage = merged
    return replace(
        options,
        configuration=configs,
        schema_mode=schema_mode,
        target_file_size=target_file_size,
        storage_options=merged_storage,
        log_storage_options=merged_log_storage,
    )


def _reserve_delta_commit(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    key: str,
    delta_options: IbisDeltaWriteOptions,
    metadata: Mapping[str, object] | None,
) -> DeltaCommitReservation:
    if delta_options.app_id is not None and delta_options.version is not None:
        return DeltaCommitReservation(options=delta_options, run=None, key=key)
    commit_options, commit_run = runtime_profile.reserve_delta_commit(
        key=key,
        metadata=metadata,
        commit_metadata=delta_options.commit_metadata,
    )
    reserved_options = replace(
        delta_options,
        app_id=commit_options.app_id,
        version=commit_options.version,
    )
    return DeltaCommitReservation(options=reserved_options, run=commit_run, key=key)


def _finalize_delta_commit(
    runtime_profile: DataFusionRuntimeProfile,
    reservation: DeltaCommitReservation,
) -> None:
    if reservation.run is None:
        return
    runtime_profile.finalize_delta_commit(key=reservation.key, run=reservation.run)


def _finalize_delta_write_result(
    *,
    path: str,
    version: int | None,
    runtime_profile: DataFusionRuntimeProfile,
    reporter: Callable[[DeltaWriteResult], None] | None,
) -> DeltaWriteResult:
    result = DeltaWriteResult(path=path, version=version)
    _record_delta_features(runtime_profile, path=path, version=version)
    if reporter is not None:
        reporter(result)
    return result


def _write_delta_ibis_from_input(
    value: IbisWriteInput,
    *,
    backend: BaseBackend,
    execution: IbisExecutionContext,
    path: str,
    options: IbisDeltaWriteOptions,
) -> int | None:
    write_source = value.materialize_table() if isinstance(value, PlanProduct) else value
    write_plan = source_to_ibis(
        cast("IbisPlan | IbisTable | TableLike | RecordBatchReaderLike", write_source),
        options=SourceToIbisOptions(
            backend=backend,
            name=None,
            ordering=None,
            namespace_recorder=namespace_recorder_from_ctx(execution.ctx),
        ),
    )
    return write_delta_ibis(backend, write_plan.expr, path, options=options)


def _write_delta_dataset(
    *,
    name: str,
    path: str,
    table_name: str | None,
    value: IbisWriteInput,
    context: DeltaWriteContext,
) -> DeltaWriteResult:
    reservation = _reserve_delta_commit(
        context.runtime_profile,
        key=path,
        delta_options=context.delta_options,
        metadata={"dataset": name},
    )
    insert_result = None
    if table_name is not None and _delta_insert_allowed(
        context.ctx,
        table_name=table_name,
        delta_options=context.delta_options,
    ):
        constraints = _delta_constraints_for_table(
            context.runtime_profile,
            table_name=table_name,
        )
        request = DeltaInsertRequest(
            ctx=context.ctx,
            table_name=table_name,
            value=value,
            mode=cast("DeltaInsertMode", context.delta_options.mode),
            backend=cast("IbisCompilerBackend", context.backend),
            batch_size=context.batch_size,
            runtime_profile=context.runtime_profile,
            constraints=constraints,
        )
        insert_result = _try_delta_insert(request)
    if insert_result is None:
        if context.runtime_profile.enable_delta_data_checker:
            resolved_name = table_name or name
            location = _delta_location_for_table(context.runtime_profile, table_name=resolved_name)
            if location is None:
                _record_delta_data_checker(
                    context.runtime_profile,
                    payload={
                        "event_time_unix_ms": int(time.time() * 1000),
                        "status": "skipped",
                        "operation": "delta_write",
                        "table_name": resolved_name,
                        "reason": "missing_location",
                    },
                )
            else:
                table = _delta_check_table_for_input(value, context=context)
                _run_delta_data_checker(
                    DeltaDataCheckContext(
                        ctx=context.ctx,
                        runtime_profile=context.runtime_profile,
                        table_name=resolved_name,
                        table=table,
                        constraints=tuple(location.delta_constraints),
                        operation="delta_write",
                        location=location,
                    )
                )
        version = _write_delta_ibis_from_input(
            value,
            backend=context.backend,
            execution=context.execution,
            path=path,
            options=reservation.options,
        )
        _finalize_delta_commit(context.runtime_profile, reservation)
        return _finalize_delta_write_result(
            path=path,
            version=version,
            runtime_profile=context.runtime_profile,
            reporter=context.reporter,
        )
    _finalize_delta_commit(context.runtime_profile, reservation)
    version = delta_table_version(
        path,
        storage_options=reservation.options.storage_options,
        log_storage_options=reservation.options.log_storage_options,
    )
    return _finalize_delta_write_result(
        path=path,
        version=version,
        runtime_profile=context.runtime_profile,
        reporter=context.reporter,
    )


def _resolved_delta_write_options(
    options: IbisDatasetWriteOptions | IbisNamedDatasetWriteOptions,
) -> IbisDeltaWriteOptions:
    delta_options = options.delta_options or IbisDeltaWriteOptions()
    return apply_ibis_delta_write_policies(
        delta_options,
        write_policy=options.delta_write_policy,
        schema_policy=options.delta_schema_policy,
        storage_options=options.storage_options,
        log_storage_options=options.delta_log_storage_options,
    )


def write_ibis_dataset_delta(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisDatasetWriteOptions | None = None,
    table_name: str | None = None,
) -> DeltaWriteResult:
    """Write an Ibis plan/table or Arrow input as a Delta table.

    Returns
    -------
    DeltaWriteResult
        Metadata about the Delta write operation.

    Raises
    ------
    ValueError
        Raised when a DataFusion writer is requested without a compatible execution context.
    ValueError
        Raised when the DataFusion backend cannot produce an Arrow table.
    """
    options = options or IbisDatasetWriteOptions()
    writer_strategy = _resolve_writer_strategy(options=options, data=data)
    if writer_strategy != "datafusion":
        msg = "Delta writes require the DataFusion writer strategy."
        raise ValueError(msg)
    if options.execution is None:
        msg = "DataFusion writer requires an execution context for Ibis inputs."
        raise ValueError(msg)
    runtime_profile = options.execution.ctx.runtime.datafusion
    backend = options.execution.ibis_backend
    if runtime_profile is None or backend is None:
        msg = "DataFusion writer requires a runtime profile and Ibis backend."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    context = DeltaWriteContext(
        execution=options.execution,
        ctx=df_ctx,
        backend=backend,
        runtime_profile=runtime_profile,
        delta_options=_resolved_delta_write_options(options),
        batch_size=_resolved_write_batch_size(options),
        reporter=options.delta_reporter,
    )
    resolved_table = table_name or _table_name_for_delta_path(df_ctx, path=str(base_dir))
    return _write_delta_dataset(
        name=resolved_table or str(base_dir),
        path=str(base_dir),
        table_name=resolved_table,
        value=data,
        context=context,
    )


def write_ibis_named_datasets_delta(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisNamedDatasetWriteOptions | None = None,
) -> dict[str, DeltaWriteResult]:
    """Write a mapping of Ibis/Arrow datasets to Delta tables.

    Returns
    -------
    dict[str, DeltaWriteResult]
        Mapping of dataset names to Delta write metadata.

    Raises
    ------
    ValueError
        Raised when a non-Arrow writer strategy is requested.
    """
    options = options or IbisNamedDatasetWriteOptions()
    writer_strategy = options.writer_strategy or "datafusion"
    if writer_strategy != "datafusion":
        msg = "Named dataset writes require the DataFusion writer strategy."
        raise ValueError(msg)
    if options.execution is None:
        msg = "Named dataset writes require an execution context."
        raise ValueError(msg)
    runtime_profile = options.execution.ctx.runtime.datafusion
    backend = options.execution.ibis_backend
    if runtime_profile is None or backend is None:
        msg = "Named dataset writes require a runtime profile and Ibis backend."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    delta_options = _resolved_delta_write_options(options)
    batch_size = _resolved_named_write_batch_size(options)
    context = DeltaWriteContext(
        execution=options.execution,
        ctx=df_ctx,
        backend=backend,
        runtime_profile=runtime_profile,
        delta_options=delta_options,
        batch_size=batch_size,
        reporter=None,
    )
    results = {
        name: _write_delta_dataset(
            name=name,
            path=str(Path(base_dir) / name),
            table_name=name,
            value=value,
            context=context,
        )
        for name, value in datasets.items()
    }
    if options.delta_reporter is not None:
        for result in results.values():
            options.delta_reporter(result)
    return results


def _datafusion_df_for_write(
    *,
    name: str,
    value: IbisWriteInput,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
    batch_size: int | None,
) -> tuple[object, str | None]:
    if isinstance(value, IbisPlan):
        return ibis_plan_to_datafusion(value, backend=backend, ctx=ctx), None
    if isinstance(value, IbisTable):
        return ibis_to_datafusion(value, backend=backend, ctx=ctx), None
    if isinstance(value, PlanProduct):
        value = value.materialize_table()
    table = coerce_table_like(value)
    temp_name = f"__delta_write_{name}_{uuid.uuid4().hex}"
    df = datafusion_from_arrow(ctx, name=temp_name, value=table, batch_size=batch_size)
    return df, temp_name


def _table_name_for_delta_path(ctx: SessionContext, *, path: str) -> str | None:
    entries = all_table_provider_metadata(id(ctx))
    for name, metadata in entries.items():
        if metadata.storage_location is None:
            continue
        if str(metadata.storage_location) == path:
            return name
    return None


def _table_registered(ctx: SessionContext, *, table_name: str) -> bool:
    try:
        ctx.table(table_name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return False
    return True


def _delta_insert_compatible(options: IbisDeltaWriteOptions) -> bool:
    return (
        options.mode in {"append", "overwrite"}
        and options.schema_mode != "overwrite"
        and options.predicate is None
        and not options.configuration
        and not options.commit_metadata
        and options.target_file_size is None
        and options.writer_properties is None
        and options.app_id is None
        and options.version is None
    )


def _delta_insert_allowed(
    ctx: SessionContext,
    *,
    table_name: str,
    delta_options: IbisDeltaWriteOptions,
) -> bool:
    metadata = table_provider_metadata(id(ctx), table_name=table_name)
    format_ok = (
        metadata is None or metadata.file_format is None or metadata.file_format.lower() == "delta"
    )
    partition_ok = (
        metadata is None
        or not metadata.partition_columns
        or not delta_options.partition_by
        or tuple(delta_options.partition_by) == metadata.partition_columns
    )
    return (
        _delta_insert_compatible(delta_options)
        and _table_registered(ctx, table_name=table_name)
        and format_ok
        and partition_ok
    )


def _is_insert_unsupported_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in ("not supported", "unsupported", "unimplemented", "not implemented")
    )


def _table_provider_payload(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    table_name: str,
) -> Mapping[str, object] | None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return None
    artifacts = runtime_profile.diagnostics_sink.artifacts_snapshot().get(
        "datafusion_table_providers_v1",
        [],
    )
    for artifact in reversed(artifacts):
        if artifact.get("name") == table_name:
            return {
                "provider": artifact.get("provider"),
                "provider_type": artifact.get("provider_type"),
                "capsule_id": artifact.get("capsule_id"),
                "projection_pushdown": artifact.get("projection_pushdown"),
                "predicate_pushdown": artifact.get("predicate_pushdown"),
                "limit_pushdown": artifact.get("limit_pushdown"),
            }
    return None


def _record_delta_insert_diagnostics(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    table_name: str,
    mode: str,
    rows_affected: int | None,
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "statement_kind": "delta_insert",
        "table_name": table_name,
        "write_mode": mode,
        "rows_affected": rows_affected,
    }
    provider_payload = _table_provider_payload(runtime_profile, table_name=table_name)
    if provider_payload is not None:
        payload["table_provider"] = dict(provider_payload)
    runtime_profile.diagnostics_sink.record_events(
        "datafusion_dml_statements_v1",
        [payload],
    )


def _record_delta_features(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    path: str,
    version: int | None,
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    try:
        features = delta_table_features(path)
        cdf_enabled = delta_cdf_enabled(path)
    except (RuntimeError, TypeError, ValueError):
        return
    payload: dict[str, object] = {
        "path": path,
        "version": version,
        "cdf_enabled": cdf_enabled,
    }
    if features is not None:
        payload["features"] = dict(features)
    runtime_profile.diagnostics_sink.record_artifact("relspec_delta_features_v1", payload)


def _record_delta_data_checker(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    payload: Mapping[str, object],
) -> None:
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    runtime_profile.diagnostics_sink.record_events("delta_data_checker_v1", [payload])


def _dataframe_to_table(df: DataFrame) -> TableLike:
    to_table = getattr(df, "to_arrow_table", None)
    if callable(to_table):
        return cast("TableLike", to_table())
    return datafusion_to_table(df)


def _delta_check_table_for_input(
    value: IbisWriteInput,
    *,
    context: DeltaWriteContext,
) -> TableLike:
    if isinstance(value, PlanProduct):
        return value.materialize_table()
    if isinstance(value, IbisPlan):
        return materialize_ibis_plan(value, execution=context.execution)
    if isinstance(value, IbisTable):
        return materialize_ibis_plan(IbisPlan(expr=value), execution=context.execution)
    table = coerce_table_like(value)
    if isinstance(table, RecordBatchReaderLike):
        return table.read_all()
    return table


def _run_delta_data_checker(context: DeltaDataCheckContext) -> None:
    if context.runtime_profile is None or not context.runtime_profile.enable_delta_data_checker:
        return
    resolved_location = context.location or _delta_location_for_table(
        context.runtime_profile, table_name=context.table_name
    )
    if resolved_location is None:
        _record_delta_data_checker(
            context.runtime_profile,
            payload={
                "event_time_unix_ms": int(time.time() * 1000),
                "status": "skipped",
                "operation": context.operation,
                "table_name": context.table_name,
                "reason": "missing_location",
            },
        )
        return
    log_storage = resolve_delta_log_storage_options(resolved_location)
    table_path = str(resolved_location.path)
    try:
        violations = delta_data_checker(
            DeltaDataCheckRequest(
                ctx=context.ctx,
                table_path=table_path,
                data=context.table,
                storage_options=resolved_location.storage_options,
                log_storage_options=log_storage,
                version=resolved_location.delta_version,
                timestamp=resolved_location.delta_timestamp,
                extra_constraints=context.constraints,
            )
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        _record_delta_data_checker(
            context.runtime_profile,
            payload={
                "event_time_unix_ms": int(time.time() * 1000),
                "status": "error",
                "operation": context.operation,
                "table_name": context.table_name,
                "table_path": table_path,
                "error": str(exc),
            },
        )
        raise
    status = "failed" if violations else "passed"
    _record_delta_data_checker(
        context.runtime_profile,
        payload={
            "event_time_unix_ms": int(time.time() * 1000),
            "status": status,
            "operation": context.operation,
            "table_name": context.table_name,
            "table_path": table_path,
            "violations": list(violations),
            "extra_constraints": list(context.constraints),
        },
    )
    if violations:
        msg = f"Delta constraints failed for {context.table_name}: {violations}"
        raise ValueError(msg)


def _delta_location_for_table(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    table_name: str,
) -> DatasetLocation | None:
    if runtime_profile is None:
        return None
    for catalog in runtime_profile.registry_catalogs.values():
        if catalog.has(table_name):
            return catalog.get(table_name)
    return None


def _delta_constraints_for_table(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    table_name: str,
) -> tuple[str, ...]:
    location = _delta_location_for_table(runtime_profile, table_name=table_name)
    if location is None:
        return ()
    return tuple(location.delta_constraints)


def _delta_insert_select_sql(
    df: DataFrame,
    *,
    temp_name: str | None,
) -> str | None:
    sql = datafusion_view_sql(df)
    if sql is not None:
        return sql
    if temp_name is None:
        return None
    return exp.select("*").from_(exp.table_(temp_name)).sql(dialect="datafusion")


def _try_delta_insert(request: DeltaInsertRequest) -> DeltaInsertResult | None:
    df_obj, temp_name = _datafusion_df_for_write(
        name=request.table_name,
        value=request.value,
        backend=request.backend,
        ctx=request.ctx,
        batch_size=request.batch_size,
    )
    df = cast("DataFrame", df_obj)
    try:
        if (
            request.runtime_profile is not None
            and request.runtime_profile.enable_delta_data_checker
        ):
            location = _delta_location_for_table(
                request.runtime_profile, table_name=request.table_name
            )
            if location is None:
                _record_delta_data_checker(
                    request.runtime_profile,
                    payload={
                        "event_time_unix_ms": int(time.time() * 1000),
                        "status": "skipped",
                        "operation": "delta_insert",
                        "table_name": request.table_name,
                        "reason": "missing_location",
                    },
                )
            else:
                table = _dataframe_to_table(df)
                _run_delta_data_checker(
                    DeltaDataCheckContext(
                        ctx=request.ctx,
                        runtime_profile=request.runtime_profile,
                        table_name=request.table_name,
                        table=table,
                        constraints=request.constraints,
                        operation="delta_insert",
                        location=location,
                    )
                )
        select_sql = _delta_insert_select_sql(df, temp_name=temp_name)
        options = DeltaInsertOptions(mode=request.mode, constraints=request.constraints)
        if select_sql is None:
            result = datafusion_insert_from_dataframe(
                request.ctx,
                request.table_name,
                df,
                options=options,
            )
        else:
            result = datafusion_insert_delta(
                request.ctx,
                request.table_name,
                select_sql,
                options=options,
            )
    except (RuntimeError, TypeError, ValueError) as exc:
        if _is_insert_unsupported_error(exc):
            return None
        raise
    finally:
        if temp_name is not None:
            adapter = DataFusionIOAdapter(
                ctx=request.ctx,
                profile=request.runtime_profile,
            )
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_name)
    _record_delta_insert_diagnostics(
        request.runtime_profile,
        table_name=request.table_name,
        mode=request.mode,
        rows_affected=result.rows_affected,
    )
    return result


def _prepare_parquet_dir(path: Path, *, overwrite: bool) -> None:
    if overwrite and path.exists():
        shutil.rmtree(path)
    path.parent.mkdir(parents=True, exist_ok=True)


def _coerce_parquet_table(
    value: IbisWriteInput,
    *,
    options: IbisParquetWriteOptions,
    name: str | None,
) -> IbisTable:
    backend = options.execution.ibis_backend
    if backend is None:
        msg = "Parquet exports require an Ibis backend."
        raise ValueError(msg)
    if isinstance(value, PlanProduct):
        value = value.materialize_table()
    if isinstance(value, IbisPlan):
        return value.expr
    if isinstance(value, IbisTable):
        return value
    plan = source_to_ibis(
        value,
        options=SourceToIbisOptions(
            backend=backend,
            name=name,
            namespace_recorder=namespace_recorder_from_ctx(options.execution.ctx),
        ),
    )
    return plan.expr


def write_ibis_dataset_parquet(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisParquetWriteOptions,
) -> str:
    """Write an Ibis dataset as a parquet directory.

    Returns
    -------
    str
        Path to the parquet dataset directory.

    Raises
    ------
    ValueError
        If the runtime profile or Ibis backend is unavailable.
    """
    from datafusion_engine.runtime import statement_sql_options_for_profile

    runtime_profile = options.execution.ctx.runtime.datafusion
    backend = options.execution.ibis_backend
    if runtime_profile is None or backend is None:
        msg = "Parquet exports require a runtime profile and Ibis backend."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    batch_size = options.execution.batch_size
    df_obj, temp_name = _datafusion_df_for_write(
        name="parquet",
        value=data,
        backend=cast("IbisCompilerBackend", backend),
        ctx=df_ctx,
        batch_size=batch_size,
    )
    df = cast("DataFrame", df_obj)
    path = Path(base_dir)
    _prepare_parquet_dir(path, overwrite=options.overwrite)
    format_options: dict[str, object] = (
        dict(options.dataset_options) if options.dataset_options is not None else {}
    )
    format_options.pop("partition_by", None)
    partition_by = _copy_partition_by(options.partition_by, df)
    request = WriteRequest(
        source=_write_source(df, temp_name=temp_name),
        destination=str(path),
        format=WriteFormat.PARQUET,
        mode=WriteMode.OVERWRITE if options.overwrite else WriteMode.ERROR,
        partition_by=partition_by,
        format_options=format_options or None,
    )
    profile = _write_pipeline_profile()
    pipeline = WritePipeline(
        df_ctx,
        profile,
        sql_options=statement_sql_options_for_profile(runtime_profile),
        recorder=recorder_for_profile(runtime_profile, operation_id="ibis_parquet_write"),
    )
    try:
        pipeline.write(request, prefer_streaming=True)
    finally:
        if temp_name is not None:
            adapter = DataFusionIOAdapter(ctx=df_ctx, profile=runtime_profile)
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_name)
    return str(path)


def write_ibis_dataset_copy(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisCopyWriteOptions,
) -> IbisCopyWriteResult:
    """Write an Ibis dataset via DataFusion COPY.

    Returns
    -------
    IbisCopyWriteResult
        COPY write metadata.
    """
    results = write_ibis_named_datasets_copy(
        {"dataset": data},
        base_dir,
        options=options,
    )
    return results["dataset"]


def write_ibis_named_datasets_copy(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisCopyWriteOptions,
) -> dict[str, IbisCopyWriteResult]:
    """Write a mapping of datasets via DataFusion COPY.

    Returns
    -------
    dict[str, IbisCopyWriteResult]
        Mapping of dataset names to COPY write metadata.

    Raises
    ------
    ValueError
        If the runtime profile or Ibis backend is unavailable.
    """
    from datafusion_engine.runtime import statement_sql_options_for_profile

    runtime_profile = options.execution.ctx.runtime.datafusion
    if runtime_profile is None or options.execution.ibis_backend is None:
        msg = "COPY writer requires a runtime profile and Ibis backend."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    backend = cast("IbisCompilerBackend", options.execution.ibis_backend)
    batch_size = options.execution.batch_size
    profile = _write_pipeline_profile()
    pipeline = WritePipeline(
        df_ctx,
        profile,
        sql_options=statement_sql_options_for_profile(runtime_profile),
        recorder=recorder_for_profile(runtime_profile, operation_id="ibis_copy_write"),
    )
    write_format = _write_format(options.file_format)
    context = _NamedCopyContext(
        base_dir=base_dir,
        options=options,
        ctx=df_ctx,
        backend=backend,
        batch_size=batch_size,
        pipeline=pipeline,
        profile=profile,
        write_format=write_format,
    )
    results: dict[str, IbisCopyWriteResult] = {}
    for name, value in datasets.items():
        results[name] = _write_named_dataset_copy(
            context,
            name=name,
            value=value,
        )
    return results


@dataclass(frozen=True)
class _NamedCopyContext:
    base_dir: PathLike
    options: IbisCopyWriteOptions
    ctx: SessionContext
    backend: IbisCompilerBackend
    batch_size: int | None
    pipeline: WritePipeline
    profile: SQLPolicyProfile
    write_format: WriteFormat


def _write_named_dataset_copy(
    context: _NamedCopyContext,
    name: str,
    value: IbisWriteInput,
) -> IbisCopyWriteResult:
    df_obj, temp_name = _datafusion_df_for_write(
        name=name,
        value=value,
        backend=context.backend,
        ctx=context.ctx,
        batch_size=context.batch_size,
    )
    df = cast("DataFrame", df_obj)
    target_path = Path(context.base_dir) / name
    _prepare_copy_dir(target_path, overwrite=context.options.overwrite)
    partition_by = _copy_partition_by(context.options.partition_by, df)
    try:
        request = WriteRequest(
            source=_write_source(df, temp_name=temp_name),
            destination=str(target_path),
            format=context.write_format,
            mode=WriteMode.OVERWRITE if context.options.overwrite else WriteMode.ERROR,
            partition_by=partition_by,
            format_options=(
                dict(context.options.statement_overrides)
                if context.options.statement_overrides
                else None
            ),
        )
        result = context.pipeline.write_via_copy(request)
        if result.sql is None:
            msg = "COPY writer failed to produce a SQL statement."
            raise ValueError(msg)
        if context.options.record_hook is not None:
            context.options.record_hook(
                {
                    "sql": result.sql,
                    "dialect": context.profile.write_dialect,
                    "policy_violations": [],
                    "sql_policy_name": None,
                    "param_mode": "none",
                }
            )
    finally:
        if temp_name is not None:
            adapter = DataFusionIOAdapter(ctx=context.ctx, profile=None)
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_name)
    return IbisCopyWriteResult(
        path=str(target_path),
        sql=result.sql,
        file_format=context.options.file_format,
        partition_by=partition_by,
        statement_overrides=context.options.statement_overrides,
    )


def _prepare_copy_dir(path: Path, *, overwrite: bool) -> None:
    if overwrite and path.exists():
        shutil.rmtree(path)
    path.parent.mkdir(parents=True, exist_ok=True)

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
        "arrow": WriteFormat.ARROW,
    }
    resolved = mapping.get(normalized)
    if resolved is None:
        msg = f"Unsupported write format: {value!r}."
        raise ValueError(msg)
    return resolved


def _write_source(
    df: DataFrame,
    *,
    temp_name: str | None,
) -> str | exp.Expression:
    sql = datafusion_view_sql(df)
    if sql is not None:
        return sql
    if temp_name is None:
        msg = "Write pipeline requires SQL for the DataFusion DataFrame."
        raise ValueError(msg)
    return exp.select("*").from_(exp.table_(temp_name))


def _copy_partition_by(
    partition_by: Sequence[str] | None,
    df: DataFrame,
) -> tuple[str, ...]:
    if not partition_by:
        return ()
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        names = schema.names
    else:
        names = getattr(schema, "names", None)
        if names is None:
            return tuple(partition_by)
    available = set(names)
    return tuple(name for name in partition_by if name in available)


def _parquet_params(
    params: Mapping[IbisValue, object] | None,
) -> Mapping[Scalar, object] | None:
    if params is None:
        return None
    resolved = {key: value for key, value in params.items() if isinstance(key, Scalar)}
    return resolved or None


def write_ibis_named_datasets_parquet(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisParquetWriteOptions,
) -> dict[str, str]:
    """Write a mapping of datasets to parquet directories.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to parquet directory paths.
    """
    results: dict[str, str] = {}
    for name, value in datasets.items():
        path = Path(base_dir) / name
        results[name] = write_ibis_dataset_parquet(
            value,
            path,
            options=options,
        )
    return results


__all__ = [
    "DeltaWriteResult",
    "IbisCopyWriteOptions",
    "IbisCopyWriteResult",
    "IbisDatasetWriteOptions",
    "IbisDeltaWriteOptions",
    "IbisMaterializeOptions",
    "IbisNamedDatasetWriteOptions",
    "IbisParquetWriteOptions",
    "IbisWriteInput",
    "apply_ibis_delta_write_policies",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "materialize_table",
    "write_ibis_dataset_copy",
    "write_ibis_dataset_delta",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_copy",
    "write_ibis_named_datasets_delta",
    "write_ibis_named_datasets_parquet",
]
