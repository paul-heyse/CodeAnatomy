"""Arrow IPC, Delta Lake, and Parquet read/write helpers."""

from __future__ import annotations

from typing import Literal

import pyarrow as pa

from arrowdsl.io.delta import (
    DeltaCdfOptions,
    DeltaSchemaMode,
    DeltaUpsertOptions,
    DeltaWriteInput,
    DeltaWriteMode,
    DeltaWriteOptions,
    DeltaWriteResult,
    DeltaWriteRetryPolicy,
    StorageOptions,
    apply_delta_write_policies,
    cleanup_delta_log,
    coerce_delta_table,
    create_delta_checkpoint,
    delta_commit_metadata,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_features,
    delta_table_version,
    enable_delta_features,
    open_delta_table,
    read_delta_cdf,
    read_table_delta,
    upsert_dataset_partitions_delta,
    vacuum_delta,
    write_dataset_delta,
    write_finalize_result_delta,
    write_named_datasets_delta,
    write_table_delta,
)
from arrowdsl.io.delta_config import DeltaSchemaPolicy, DeltaWritePolicy
from arrowdsl.io.ipc import read_table_ipc_file, write_table_ipc_file, write_table_ipc_stream
from arrowdsl.io.parquet import (
    DatasetWriteConfig,
    DatasetWriteInput,
    NamedDatasetWriteConfig,
    ParquetMetadataConfig,
    ParquetWriteOptions,
    read_table_parquet,
    upsert_dataset_partitions_parquet,
    write_dataset_parquet,
    write_finalize_result_parquet,
    write_named_datasets_parquet,
    write_parquet_metadata_sidecars,
    write_partitioned_dataset_parquet,
    write_table_parquet,
)
from core_types import PathLike, ensure_path
from ibis_engine.io_bridge import (
    IbisWriteInput,
    ibis_plan_to_reader,
    ibis_table_to_reader,
    ibis_to_table,
    write_ibis_dataset_delta,
    write_ibis_dataset_parquet,
    write_ibis_named_datasets_delta,
    write_ibis_named_datasets_parquet,
    write_ibis_table_delta,
    write_ibis_table_parquet,
)


def mmap_file(path: PathLike, *, mode: Literal["r", "r+", "w+", "a+"] = "r") -> pa.MemoryMappedFile:
    """Return a memory-mapped file handle for a path.

    Returns
    -------
    pyarrow.MemoryMappedFile
        Memory-mapped file handle.
    """
    target = ensure_path(path)
    return pa.memory_map(str(target), mode)


def open_compressed_input(
    path: PathLike,
    *,
    compression: str,
) -> pa.CompressedInputStream:
    """Return a compressed input stream for a path.

    Returns
    -------
    pyarrow.CompressedInputStream
        Compressed input stream.
    """
    target = ensure_path(path)
    return pa.CompressedInputStream(pa.input_stream(str(target)), compression)


def open_compressed_output(
    path: PathLike,
    *,
    compression: str,
) -> pa.CompressedOutputStream:
    """Return a compressed output stream for a path.

    Returns
    -------
    pyarrow.CompressedOutputStream
        Compressed output stream.
    """
    target = ensure_path(path)
    return pa.CompressedOutputStream(pa.output_stream(str(target)), compression)


__all__ = [
    "DatasetWriteConfig",
    "DatasetWriteInput",
    "DeltaCdfOptions",
    "DeltaSchemaMode",
    "DeltaSchemaPolicy",
    "DeltaUpsertOptions",
    "DeltaWriteInput",
    "DeltaWriteMode",
    "DeltaWriteOptions",
    "DeltaWritePolicy",
    "DeltaWriteResult",
    "DeltaWriteRetryPolicy",
    "IbisWriteInput",
    "NamedDatasetWriteConfig",
    "ParquetMetadataConfig",
    "ParquetWriteOptions",
    "StorageOptions",
    "apply_delta_write_policies",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_commit_metadata",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "enable_delta_features",
    "ibis_plan_to_reader",
    "ibis_table_to_reader",
    "ibis_to_table",
    "mmap_file",
    "open_compressed_input",
    "open_compressed_output",
    "open_delta_table",
    "read_delta_cdf",
    "read_table_delta",
    "read_table_ipc_file",
    "read_table_parquet",
    "upsert_dataset_partitions_delta",
    "upsert_dataset_partitions_parquet",
    "vacuum_delta",
    "write_dataset_delta",
    "write_dataset_parquet",
    "write_finalize_result_delta",
    "write_finalize_result_parquet",
    "write_ibis_dataset_delta",
    "write_ibis_dataset_parquet",
    "write_ibis_named_datasets_delta",
    "write_ibis_named_datasets_parquet",
    "write_ibis_table_delta",
    "write_ibis_table_parquet",
    "write_named_datasets_delta",
    "write_named_datasets_parquet",
    "write_parquet_metadata_sidecars",
    "write_partitioned_dataset_parquet",
    "write_table_delta",
    "write_table_ipc_file",
    "write_table_ipc_stream",
    "write_table_parquet",
]
