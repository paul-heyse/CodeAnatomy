"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from ibis.expr.types import Table as IbisTable

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    DatasetWriteInput,
    NamedDatasetWriteConfig,
    ParquetWriteOptions,
    write_dataset_parquet,
    write_named_datasets_parquet,
    write_table_parquet,
)
from core_types import PathLike
from ibis_engine.execution import (
    IbisAdapterExecution,
    materialize_ibis_plan,
    stream_ibis_plan,
)
from ibis_engine.plan import IbisPlan

type IbisWriteInput = DatasetWriteInput | IbisPlan | IbisTable


@dataclass(frozen=True)
class IbisDatasetWriteOptions:
    """Options for writing a single dataset from Ibis inputs."""

    config: DatasetWriteConfig | None = None
    batch_size: int | None = None
    prefer_reader: bool = True
    execution: IbisAdapterExecution | None = None


@dataclass(frozen=True)
class IbisNamedDatasetWriteOptions:
    """Options for writing named datasets from Ibis inputs."""

    config: NamedDatasetWriteConfig | None = None
    batch_size: int | None = None
    prefer_reader: bool = True
    execution: IbisAdapterExecution | None = None


def ibis_plan_to_reader(
    plan: IbisPlan,
    *,
    batch_size: int | None = None,
    execution: IbisAdapterExecution | None = None,
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
    execution: IbisAdapterExecution | None = None,
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
    execution: IbisAdapterExecution | None,
) -> DatasetWriteInput:
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


def write_ibis_table_parquet(
    table: IbisPlan | IbisTable,
    path: PathLike,
    *,
    opts: ParquetWriteOptions | None = None,
    overwrite: bool = True,
    execution: IbisAdapterExecution | None = None,
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
    """
    options = options or IbisDatasetWriteOptions()
    value = _coerce_write_input(
        data,
        batch_size=options.batch_size,
        prefer_reader=options.prefer_reader,
        execution=options.execution,
    )
    return write_dataset_parquet(value, base_dir, config=options.config)


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
    """
    options = options or IbisNamedDatasetWriteOptions()
    converted = {
        name: _coerce_write_input(
            value,
            batch_size=options.batch_size,
            prefer_reader=options.prefer_reader,
            execution=options.execution,
        )
        for name, value in datasets.items()
    }
    return write_named_datasets_parquet(converted, base_dir, config=options.config)


__all__ = [
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
