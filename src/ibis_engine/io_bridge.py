"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import cast

from datafusion import DataFrameWriteOptions, ParquetWriterOptions, col
from ibis.expr.types import Table as IbisTable

from arrowdsl.core.context import DeterminismTier
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    DatasetWriteInput,
    NamedDatasetWriteConfig,
    ParquetWriteOptions,
    write_dataset_parquet,
    write_named_datasets_parquet,
    write_partitioned_dataset_parquet,
    write_table_parquet,
)
from arrowdsl.plan.ordering_policy import ordering_keys_for_schema
from arrowdsl.plan.schema_utils import plan_schema
from core_types import PathLike
from datafusion_engine.bridge import (
    datafusion_partitioned_readers,
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
from sqlglot_tools.bridge import IbisCompilerBackend

type IbisWriteInput = DatasetWriteInput | IbisPlan | IbisTable | PlanProduct

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataFusionWriterConfig:
    """Options for DataFusion native writers."""

    write_options: DataFrameWriteOptions | None = None
    parquet_options: ParquetWriterOptions | None = None
    sort_by: Sequence[str] | None = None
    partitioned_streaming: bool = False
    allow_non_deterministic_partitioned_streaming: bool = False


@dataclass(frozen=True)
class IbisDatasetWriteOptions:
    """Options for writing a single dataset from Ibis inputs."""

    config: DatasetWriteConfig | None = None
    batch_size: int | None = None
    prefer_reader: bool = True
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None
    datafusion_write: DataFusionWriterConfig | None = None


@dataclass(frozen=True)
class IbisNamedDatasetWriteOptions:
    """Options for writing named datasets from Ibis inputs."""

    config: NamedDatasetWriteConfig | None = None
    batch_size: int | None = None
    prefer_reader: bool = True
    execution: IbisExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None


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
        return table.to_pyarrow_batches()
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


def _coerce_write_input(
    value: IbisWriteInput,
    *,
    batch_size: int | None,
    prefer_reader: bool,
    execution: IbisExecutionContext | None,
) -> DatasetWriteInput:
    if isinstance(value, PlanProduct):
        return value.value()
    if isinstance(value, IbisPlan):
        return (
            ibis_plan_to_reader(value, batch_size=batch_size, execution=execution)
            if prefer_reader
            else ibis_to_table(value, execution=execution)
        )
    if isinstance(value, IbisTable):
        return (
            ibis_table_to_reader(value, batch_size=batch_size)
            if prefer_reader
            else value.to_pyarrow()
        )
    return value


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


def _write_datafusion_dataset(
    value: IbisPlan | IbisTable,
    base_dir: PathLike,
    *,
    execution: IbisExecutionContext,
    dataset_config: DatasetWriteConfig,
    write_config: DataFusionWriterConfig | None,
) -> str:
    runtime_profile = execution.ctx.runtime.datafusion
    if runtime_profile is None or execution.ibis_backend is None:
        msg = "DataFusion writer requires a runtime profile and Ibis backend."
        raise ValueError(msg)
    df_ctx = runtime_profile.session_context()
    backend = cast("IbisCompilerBackend", execution.ibis_backend)
    df = (
        ibis_plan_to_datafusion(value, backend=backend, ctx=df_ctx)
        if isinstance(value, IbisPlan)
        else ibis_to_datafusion(value, backend=backend, ctx=df_ctx)
    )
    if write_config is not None and write_config.partitioned_streaming:
        if (
            execution.ctx.determinism != DeterminismTier.BEST_EFFORT
            and not write_config.allow_non_deterministic_partitioned_streaming
        ):
            msg = "Partitioned streaming is restricted to Tier 0 determinism by default."
            raise ValueError(msg)
        readers = datafusion_partitioned_readers(df)
        if not readers:
            msg = "Partitioned streaming requested but DataFusion does not support it."
            raise ValueError(msg)
        return write_partitioned_dataset_parquet(readers, base_dir, config=dataset_config)
    write_with_options = getattr(df, "write_parquet_with_options", None)
    if callable(write_with_options):
        write_options = _build_datafusion_write_options(
            value,
            execution=execution,
            write_config=write_config,
        )
        parquet_options = _build_parquet_write_options(
            dataset_config=dataset_config,
            write_config=write_config,
        )
        if write_options is not None or parquet_options is not None:
            write_with_options(str(base_dir), parquet_options, write_options)
            return str(base_dir)
    write_parquet = getattr(df, "write_parquet", None)
    if callable(write_parquet):
        write_parquet(str(base_dir))
        return str(base_dir)
    msg = "DataFusion writer is unavailable for the configured backend."
    raise ValueError(msg)


def _build_datafusion_write_options(
    value: IbisPlan | IbisTable,
    *,
    execution: IbisExecutionContext,
    write_config: DataFusionWriterConfig | None,
) -> DataFrameWriteOptions | None:
    if write_config is not None and write_config.write_options is not None:
        return write_config.write_options
    sort_by = None
    if write_config is not None and write_config.sort_by is not None:
        sort_by = list(write_config.sort_by)
    elif execution.ctx.determinism == DeterminismTier.CANONICAL:
        schema = _schema_for_write(value, execution=execution)
        sort_by = [col for col, _ in ordering_keys_for_schema(schema)]
    if not sort_by:
        return None
    return _create_write_options(sort_by=sort_by)


def _create_write_options(*, sort_by: Sequence[str]) -> DataFrameWriteOptions | None:
    try:
        sort_exprs = [col(name) for name in sort_by]
        return DataFrameWriteOptions(sort_by=sort_exprs)
    except TypeError:
        logger.warning("DataFrameWriteOptions does not accept sort_by; skipping ordering.")
        return None


def _build_parquet_write_options(
    *,
    dataset_config: DatasetWriteConfig,
    write_config: DataFusionWriterConfig | None,
) -> ParquetWriterOptions | None:
    if write_config is not None and write_config.parquet_options is not None:
        return write_config.parquet_options
    opts = dataset_config.opts
    if opts is None:
        return None
    return _create_parquet_options(opts)


def _create_parquet_options(options: ParquetWriteOptions) -> ParquetWriterOptions | None:
    try:
        return ParquetWriterOptions(compression=options.compression)
    except TypeError:
        logger.warning("ParquetWriterOptions constructor mismatch; using DataFusion defaults.")
        return None


def _schema_for_write(
    value: IbisPlan | IbisTable,
    *,
    execution: IbisExecutionContext,
) -> SchemaLike:
    if isinstance(value, IbisPlan):
        return plan_schema(value, ctx=execution.ctx)
    return value.schema().to_pyarrow()


def write_ibis_table_parquet(
    table: IbisPlan | IbisTable,
    path: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
    execution: IbisExecutionContext | None = None,
) -> str:
    """Write an Ibis plan/table as a single Parquet file.

    Returns
    -------
    str
        Written file path.
    """
    return write_table_parquet(
        ibis_to_table(table, execution=execution),
        path,
        opts=opts,
        overwrite=overwrite,
    )


def write_ibis_dataset_parquet(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    options: IbisDatasetWriteOptions | None = None,
) -> str:
    """Write an Ibis plan/table or Arrow input as a Parquet dataset.

    Returns
    -------
    str
        Output dataset directory.

    Raises
    ------
    ValueError
        Raised when a DataFusion writer is requested without required inputs.
    """
    options = options or IbisDatasetWriteOptions()
    config = options.config or DatasetWriteConfig()
    determinism = None
    if isinstance(data, PlanProduct):
        determinism = data.determinism_tier
    elif options.execution is not None:
        determinism = options.execution.ctx.determinism
    if determinism is not None and config.preserve_order is None:
        preserve = determinism != DeterminismTier.BEST_EFFORT
        config = replace(config, preserve_order=preserve)
    writer_strategy = _resolve_writer_strategy(options=options, data=data)
    if writer_strategy == "datafusion":
        if options.execution is None or not isinstance(data, (IbisPlan, IbisTable)):
            msg = "DataFusion writer requires an Ibis plan/table and execution context."
            raise ValueError(msg)
        return _write_datafusion_dataset(
            data,
            base_dir,
            execution=options.execution,
            dataset_config=config,
            write_config=options.datafusion_write,
        )
    value = _coerce_write_input(
        data,
        batch_size=options.batch_size,
        prefer_reader=options.prefer_reader,
        execution=options.execution,
    )
    return write_dataset_parquet(value, base_dir, config=config)


def write_ibis_named_datasets_parquet(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    options: IbisNamedDatasetWriteOptions | None = None,
) -> dict[str, str]:
    """Write a mapping of Ibis/Arrow datasets to Parquet directories.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to output directories.

    Raises
    ------
    ValueError
        Raised when a non-Arrow writer strategy is requested.
    """
    options = options or IbisNamedDatasetWriteOptions()
    config = options.config or NamedDatasetWriteConfig()
    if options.execution is not None and config.preserve_order is None:
        preserve = options.execution.ctx.determinism != DeterminismTier.BEST_EFFORT
        config = replace(config, preserve_order=preserve)
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
            execution=options.execution,
        )
        for name, value in datasets.items()
    }
    return write_named_datasets_parquet(converted, base_dir, config=config)


__all__ = [
    "DataFusionWriterConfig",
    "IbisDatasetWriteOptions",
    "IbisNamedDatasetWriteOptions",
    "IbisWriteInput",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_parquet",
    "write_ibis_table_parquet",
]
