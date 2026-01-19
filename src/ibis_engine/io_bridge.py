"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Protocol, cast

import ibis
import pyarrow as pa
from deltalake import DeltaTable
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.interop import (
    RecordBatchReaderLike,
    TableLike,
    coerce_table_like,
)
from arrowdsl.core.streaming import to_reader
from core_types import PathLike
from datafusion_engine.bridge import (
    ibis_plan_to_datafusion,
    ibis_to_datafusion,
)
from engine.plan_policy import WriterStrategy
from engine.plan_product import PlanProduct
from ibis_engine.execution import (
    IbisExecutionContext,
    materialize_ibis_plan,
    stream_ibis_plan,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.runner import async_stream_plan
from ibis_engine.sources import (
    DatabaseHint,
    record_namespace_action,
    resolve_database_hint,
)
from sqlglot_tools.bridge import IbisCompilerBackend
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    StorageOptions,
    apply_delta_write_policies,
    delta_table_version,
    write_dataset_delta,
    write_named_datasets_delta,
    write_table_delta,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

type IbisWriteInput = TableLike | RecordBatchReaderLike | IbisPlan | IbisTable | PlanProduct


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
    delta_options: DeltaWriteOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class IbisNamedDatasetWriteOptions:
    """Options for writing named datasets from Ibis inputs."""

    batch_size: int | None = None
    prefer_reader: bool = True
    use_async_streaming: bool = False
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None
    delta_reporter: Callable[[DeltaWriteResult], None] | None = None
    delta_options: DeltaWriteOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    storage_options: StorageOptions | None = None


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
    return "arrow"


def write_ibis_table_delta(
    table: IbisPlan | IbisTable,
    path: PathLike,
    *,
    options: DeltaWriteOptions | None = None,
    storage_options: StorageOptions | None = None,
    execution: IbisExecutionContext | None = None,
) -> DeltaWriteResult:
    """Write an Ibis plan/table as a Delta table.

    Returns
    -------
    DeltaWriteResult
        Metadata about the Delta write operation.
    """
    resolved = options or DeltaWriteOptions()
    return write_table_delta(
        ibis_to_table(table, execution=execution),
        str(path),
        options=resolved,
        storage_options=storage_options,
    )


def write_ibis_dataset_delta(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisDatasetWriteOptions | None = None,
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
    delta_options = options.delta_options or DeltaWriteOptions()
    delta_options = apply_delta_write_policies(
        delta_options,
        write_policy=options.delta_write_policy,
        schema_policy=options.delta_schema_policy,
    )
    writer_strategy = _resolve_writer_strategy(options=options, data=data)
    if writer_strategy == "datafusion":
        if options.execution is None or not isinstance(data, (IbisPlan, IbisTable)):
            msg = "DataFusion writer requires an Ibis plan/table and execution context."
            raise ValueError(msg)
        runtime_profile = options.execution.ctx.runtime.datafusion
        if runtime_profile is None or options.execution.ibis_backend is None:
            msg = "DataFusion writer requires a runtime profile and Ibis backend."
            raise ValueError(msg)
        df_ctx = runtime_profile.session_context()
        backend = cast("IbisCompilerBackend", options.execution.ibis_backend)
        df = (
            ibis_plan_to_datafusion(data, backend=backend, ctx=df_ctx)
            if isinstance(data, IbisPlan)
            else ibis_to_datafusion(data, backend=backend, ctx=df_ctx)
        )
        datafusion_result = _write_datafusion_delta(
            df,
            base_dir=str(base_dir),
            delta_options=delta_options,
            storage_options=options.storage_options,
        )
        if datafusion_result is not None:
            if options.delta_reporter is not None:
                options.delta_reporter(datafusion_result)
            return datafusion_result
        to_arrow = getattr(df, "to_arrow_table", None)
        if not callable(to_arrow):
            msg = "DataFusion DataFrame missing to_arrow_table."
            raise ValueError(msg)
        table = cast("TableLike", to_arrow())
        result = write_table_delta(
            table,
            str(base_dir),
            options=delta_options,
            storage_options=options.storage_options,
        )
        if options.delta_reporter is not None:
            options.delta_reporter(result)
        return result
    value = _coerce_write_input(
        data,
        batch_size=options.batch_size,
        prefer_reader=options.prefer_reader,
        use_async_streaming=options.use_async_streaming,
        execution=options.execution,
    )
    result = write_dataset_delta(
        value,
        str(base_dir),
        options=delta_options,
        storage_options=options.storage_options,
    )
    if options.delta_reporter is not None:
        options.delta_reporter(result)
    return result


def _write_datafusion_delta(
    df: object,
    *,
    base_dir: str,
    delta_options: DeltaWriteOptions,
    storage_options: StorageOptions | None,
) -> DeltaWriteResult | None:
    if not _supports_datafusion_delta_insert(delta_options):
        return None
    if delta_table_version(base_dir, storage_options=storage_options) is None:
        return None
    write_table = getattr(df, "write_table", None)
    if not callable(write_table):
        return None
    storage = dict(storage_options) if storage_options is not None else None
    table = DeltaTable(base_dir, storage_options=storage)
    name = f"__delta_sink_{uuid.uuid4().hex}"
    try:
        _register_datafusion_delta_table(df, name=name, table=table)
    except ValueError:
        return None
    try:
        write_table(name)
    finally:
        _deregister_datafusion_table(df, name=name)
    version = delta_table_version(base_dir, storage_options=storage_options)
    return DeltaWriteResult(path=base_dir, version=version)


def _supports_datafusion_delta_insert(options: DeltaWriteOptions) -> bool:
    disallowed = (
        options.mode != "append",
        options.schema_mode is not None,
        options.predicate is not None,
        bool(options.partition_by),
        bool(options.configuration),
        bool(options.commit_metadata),
        options.target_file_size is not None,
        options.writer_properties is not None,
        options.retry_policy is not None,
    )
    return not any(disallowed)


def _register_datafusion_delta_table(
    df: object,
    *,
    name: str,
    table: DeltaTable,
) -> None:
    ctx = None
    for attr in ("_ctx", "_context", "context", "ctx", "session_context"):
        ctx = getattr(df, attr, None)
        if ctx is not None:
            break
    if ctx is None:
        msg = "DataFusion DataFrame missing SessionContext."
        raise ValueError(msg)
    try:
        ctx.register_table(name, table)
    except TypeError as exc:
        msg = "DataFusion failed to register Delta table provider."
        raise ValueError(msg) from exc


def _deregister_datafusion_table(df: object, *, name: str) -> None:
    ctx = None
    for attr in ("_ctx", "_context", "context", "ctx", "session_context"):
        ctx = getattr(df, attr, None)
        if ctx is not None:
            break
    if ctx is None:
        return
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        deregister(name)


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
    writer_strategy = options.writer_strategy or "arrow"
    if writer_strategy != "arrow":
        msg = "Named dataset writes only support the Arrow writer strategy."
        raise ValueError(msg)
    for name, value in datasets.items():
        if isinstance(value, PlanProduct) and value.writer_strategy != "arrow":
            msg = (
                "Named dataset writes do not support non-Arrow PlanProduct writer strategy "
                f"for dataset {name!r}."
            )
            raise ValueError(msg)
    converted = {
        name: _coerce_write_input(
            value,
            batch_size=options.batch_size,
            prefer_reader=options.prefer_reader,
            use_async_streaming=options.use_async_streaming,
            execution=options.execution,
        )
        for name, value in datasets.items()
    }
    delta_options = options.delta_options or DeltaWriteOptions()
    delta_options = apply_delta_write_policies(
        delta_options,
        write_policy=options.delta_write_policy,
        schema_policy=options.delta_schema_policy,
    )
    results = write_named_datasets_delta(
        converted,
        str(base_dir),
        options=delta_options,
        storage_options=options.storage_options,
    )
    if options.delta_reporter is not None:
        for result in results.values():
            options.delta_reporter(result)
    return results


__all__ = [
    "DeltaWriteOptions",
    "DeltaWriteResult",
    "IbisDatasetWriteOptions",
    "IbisMaterializeOptions",
    "IbisNamedDatasetWriteOptions",
    "IbisWriteInput",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "materialize_table",
    "write_ibis_dataset_delta",
    "write_ibis_named_datasets_delta",
    "write_ibis_table_delta",
]
