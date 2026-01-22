"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

import asyncio
import shutil
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import ibis
import pyarrow as pa
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
    datafusion_from_arrow,
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
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    record_namespace_action,
    resolve_database_hint,
    source_to_ibis,
)
from sqlglot_tools.bridge import IbisCompilerBackend
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    StorageOptions,
    apply_delta_write_policies,
    write_datafusion_delta,
    write_table_delta,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

type IbisWriteInput = TableLike | RecordBatchReaderLike | IbisPlan | IbisTable | PlanProduct

if TYPE_CHECKING:
    from datafusion import SessionContext


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


@dataclass(frozen=True)
class IbisParquetWriteOptions:
    """Options for writing Ibis expressions to parquet directories."""

    execution: IbisExecutionContext
    overwrite: bool = True
    partition_by: Sequence[str] | None = None
    dataset_options: Mapping[str, object] | None = None


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


def _resolve_delta_write_input(
    data: IbisWriteInput,
    *,
    execution: IbisExecutionContext | None,
    batch_size: int | None,
    prefer_reader: bool,
) -> TableLike | RecordBatchReaderLike:
    if isinstance(data, PlanProduct):
        data = data.materialize_table()
    if isinstance(data, IbisPlan):
        if execution is None:
            msg = "Delta writes for Ibis plans require an execution context."
            raise ValueError(msg)
        if prefer_reader:
            return ibis_plan_to_reader(data, batch_size=batch_size, execution=execution)
        return ibis_to_table(data, execution=execution)
    if isinstance(data, IbisTable):
        if prefer_reader:
            return ibis_table_to_reader(data, batch_size=batch_size)
        return ibis_to_table(data, execution=execution)
    return coerce_table_like(data)


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
    if writer_strategy != "datafusion":
        msg = "Delta writes require the DataFusion writer strategy."
        raise ValueError(msg)
    datafusion_result: DeltaWriteResult | None = None
    if isinstance(data, (IbisPlan, IbisTable)):
        if options.execution is None:
            msg = "DataFusion writer requires an execution context for Ibis inputs."
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
        try:
            datafusion_result = write_datafusion_delta(
                df,
                base_dir=str(base_dir),
                options=delta_options,
                storage_options=options.storage_options,
            )
        except (TypeError, ValueError):
            datafusion_result = None
    if datafusion_result is None:
        delta_input = _resolve_delta_write_input(
            data,
            execution=options.execution,
            batch_size=options.batch_size,
            prefer_reader=options.prefer_reader,
        )
        datafusion_result = write_table_delta(
            delta_input,
            str(base_dir),
            options=delta_options,
            storage_options=options.storage_options,
        )
    if options.delta_reporter is not None:
        options.delta_reporter(datafusion_result)
    return datafusion_result


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
    if runtime_profile is None or options.execution.ibis_backend is None:
        msg = "Named dataset writes require a runtime profile and Ibis backend."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    backend = cast("IbisCompilerBackend", options.execution.ibis_backend)
    delta_options = options.delta_options or DeltaWriteOptions()
    delta_options = apply_delta_write_policies(
        delta_options,
        write_policy=options.delta_write_policy,
        schema_policy=options.delta_schema_policy,
    )
    results: dict[str, DeltaWriteResult] = {}
    for name, value in datasets.items():
        df, temp_name = _datafusion_df_for_write(
            name=name,
            value=value,
            backend=backend,
            ctx=df_ctx,
            batch_size=options.batch_size,
        )
        try:
            try:
                result = write_datafusion_delta(
                    df,
                    base_dir=f"{str(base_dir).rstrip('/')}/{name}",
                    options=delta_options,
                    storage_options=options.storage_options,
                )
            except (TypeError, ValueError):
                delta_input = _resolve_delta_write_input(
                    value,
                    execution=options.execution,
                    batch_size=options.batch_size,
                    prefer_reader=options.prefer_reader,
                )
                result = write_table_delta(
                    delta_input,
                    f"{str(base_dir).rstrip('/')}/{name}",
                    options=delta_options,
                    storage_options=options.storage_options,
                )
        finally:
            if temp_name is not None:
                _deregister_table(df_ctx, name=temp_name)
        results[name] = result
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
    """
    table = _coerce_parquet_table(data, options=options, name=None)
    path = Path(base_dir)
    _prepare_parquet_dir(path, overwrite=options.overwrite)
    dataset_options = dict(options.dataset_options) if options.dataset_options is not None else {}
    if options.partition_by is not None:
        dataset_options.setdefault("partition_by", list(options.partition_by))
    table.to_parquet_dir(
        str(path),
        params=options.execution.params,
        **dataset_options,
    )
    return str(path)


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


def _deregister_table(ctx: SessionContext, *, name: str) -> None:
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        deregister(name)


__all__ = [
    "DeltaWriteOptions",
    "DeltaWriteResult",
    "IbisDatasetWriteOptions",
    "IbisMaterializeOptions",
    "IbisNamedDatasetWriteOptions",
    "IbisParquetWriteOptions",
    "IbisWriteInput",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "materialize_table",
    "write_ibis_dataset_delta",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_delta",
    "write_ibis_named_datasets_parquet",
]
