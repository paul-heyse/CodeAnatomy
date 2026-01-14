"""Arrow IPC and Parquet read/write helpers."""

from __future__ import annotations

from arrowdsl.io.ipc import read_table_ipc_file, write_table_ipc_file, write_table_ipc_stream
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    DatasetWriteInput,
    NamedDatasetWriteConfig,
    ParquetMetadataConfig,
    ParquetWriteOptions,
    read_table_parquet,
    write_dataset_parquet,
    write_finalize_result_parquet,
    write_named_datasets_parquet,
    write_parquet_metadata_sidecars,
    write_table_parquet,
)
from ibis_engine.io_bridge import (
    IbisWriteInput,
    ibis_plan_to_reader,
    ibis_table_to_reader,
    ibis_to_table,
    write_ibis_dataset_parquet,
    write_ibis_named_datasets_parquet,
    write_ibis_table_parquet,
)

__all__ = [
    "DatasetWriteConfig",
    "DatasetWriteInput",
    "IbisWriteInput",
    "NamedDatasetWriteConfig",
    "ParquetMetadataConfig",
    "ParquetWriteOptions",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "read_table_ipc_file",
    "read_table_parquet",
    "write_dataset_parquet",
    "write_finalize_result_parquet",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_parquet",
    "write_ibis_table_parquet",
    "write_named_datasets_parquet",
    "write_parquet_metadata_sidecars",
    "write_table_ipc_file",
    "write_table_ipc_stream",
    "write_table_parquet",
]
