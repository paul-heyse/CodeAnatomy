"""Ibis IO bridge helpers for Arrow materialization."""

from __future__ import annotations

from collections.abc import Mapping

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
from ibis_engine.plan import IbisPlan

type IbisWriteInput = DatasetWriteInput | IbisPlan | IbisTable


def ibis_plan_to_reader(plan: IbisPlan, *, batch_size: int | None = None) -> RecordBatchReaderLike:
    """Return a RecordBatchReader for an Ibis plan.

    Returns
    -------
    RecordBatchReaderLike
        Reader yielding batches from the plan.
    """
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


def ibis_to_table(value: IbisPlan | IbisTable) -> TableLike:
    """Materialize an Ibis plan or table to an Arrow table.

    Returns
    -------
    TableLike
        Materialized Arrow table.
    """
    if isinstance(value, IbisPlan):
        return value.to_table()
    return value.to_pyarrow()


def _coerce_write_input(
    value: IbisWriteInput,
    *,
    batch_size: int | None,
    prefer_reader: bool,
) -> DatasetWriteInput:
    if isinstance(value, IbisPlan):
        return (
            ibis_plan_to_reader(value, batch_size=batch_size) if prefer_reader else value.to_table()
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
) -> str:
    """Write an Ibis plan/table as a single Parquet file.

    Returns
    -------
    str
        Written file path.
    """
    return write_table_parquet(ibis_to_table(table), path, opts=opts, overwrite=overwrite)


def write_ibis_dataset_parquet(
    data: IbisWriteInput,
    base_dir: PathLike,
    *,
    config: DatasetWriteConfig | None = None,
    batch_size: int | None = None,
    prefer_reader: bool = True,
) -> str:
    """Write an Ibis plan/table or Arrow input as a Parquet dataset.

    Returns
    -------
    str
        Output dataset directory.
    """
    value = _coerce_write_input(data, batch_size=batch_size, prefer_reader=prefer_reader)
    return write_dataset_parquet(value, base_dir, config=config)


def write_ibis_named_datasets_parquet(
    datasets: Mapping[str, IbisWriteInput],
    base_dir: PathLike,
    *,
    config: NamedDatasetWriteConfig | None = None,
    batch_size: int | None = None,
    prefer_reader: bool = True,
) -> dict[str, str]:
    """Write a mapping of Ibis/Arrow datasets to Parquet directories.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to output directories.
    """
    converted = {
        name: _coerce_write_input(value, batch_size=batch_size, prefer_reader=prefer_reader)
        for name, value in datasets.items()
    }
    return write_named_datasets_parquet(converted, base_dir, config=config)


__all__ = [
    "IbisWriteInput",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_parquet",
    "write_ibis_table_parquet",
]
