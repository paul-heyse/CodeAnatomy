"""Arrow IPC read/write helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.io import IpcWriteConfig, ipc_read_options_factory, ipc_write_options_factory
from core_types import JsonDict, PathLike, ensure_path
from engine.plan_product import PlanProduct

type IpcWriteInput = TableLike | RecordBatchReaderLike | PlanProduct


def _coerce_plan_product(value: IpcWriteInput) -> TableLike | RecordBatchReaderLike:
    if isinstance(value, PlanProduct):
        if value.writer_strategy != "arrow":
            msg = "IPC writes require an Arrow writer strategy."
            raise ValueError(msg)
        return value.value()
    return value


def _ensure_dir(path: Path) -> None:
    """Ensure the directory exists for a target path.

    Parameters
    ----------
    path
        Directory path to create if missing.
    """
    path.mkdir(exist_ok=True, parents=True)


def write_table_ipc_file(
    table: IpcWriteInput,
    path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
    config: IpcWriteConfig | None = None,
) -> str:
    """Write an Arrow IPC file (random-access) to a path.

    Returns
    -------
    str
        Path to the written IPC file.
    """
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    data = _coerce_plan_product(table)
    schema = data.schema
    if schema_metadata:
        schema = SchemaMetadataSpec(schema_metadata=schema_metadata).apply(schema)

    options = ipc_write_options_factory(config)
    with (
        pa.OSFile(str(target), "wb") as sink,
        pa_ipc.new_file(sink, schema, options=options) as writer,
    ):
        if isinstance(data, RecordBatchReaderLike):
            for batch in data:
                writer.write_batch(batch)
        else:
            table_value = data.cast(schema, safe=False)
            writer.write_table(table_value)
    return str(target)


def write_table_ipc_stream(
    table: IpcWriteInput,
    path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
    config: IpcWriteConfig | None = None,
) -> str:
    """Write an Arrow IPC stream (sequential) to a path.

    Returns
    -------
    str
        Path to the written IPC stream.
    """
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    data = _coerce_plan_product(table)
    schema = data.schema
    if schema_metadata:
        schema = SchemaMetadataSpec(schema_metadata=schema_metadata).apply(schema)

    options = ipc_write_options_factory(config)
    with (
        pa.OSFile(str(target), "wb") as sink,
        pa_ipc.new_stream(sink, schema, options=options) as writer,
    ):
        if isinstance(data, RecordBatchReaderLike):
            for batch in data:
                writer.write_batch(batch)
        else:
            table_value = data.cast(schema, safe=False)
            writer.write_table(table_value)
    return str(target)


def write_ipc_bundle(
    table: IpcWriteInput,
    base_path: PathLike,
    *,
    overwrite: bool = True,
    schema_metadata: dict[bytes, bytes] | None = None,
    config: IpcWriteConfig | None = None,
) -> dict[str, str]:
    """Write IPC file and stream artifacts for repro bundles.

    Returns
    -------
    dict[str, str]
        Mapping with ``file`` and ``stream`` artifact paths.
    """
    base = ensure_path(base_path)
    file_path = base.with_suffix(".arrow")
    stream_path = base.with_suffix(".arrows")
    return {
        "file": write_table_ipc_file(
            table,
            file_path,
            overwrite=overwrite,
            schema_metadata=schema_metadata,
            config=config,
        ),
        "stream": write_table_ipc_stream(
            table,
            stream_path,
            overwrite=overwrite,
            schema_metadata=schema_metadata,
            config=config,
        ),
    }


def ipc_write_config_payload(config: IpcWriteConfig | None) -> JsonDict:
    """Return a JSON-friendly payload for IPC write options.

    Returns
    -------
    JsonDict
        JSON-ready IPC write configuration payload.
    """
    resolved = config or IpcWriteConfig()
    return {
        "compression": resolved.compression,
        "use_threads": resolved.use_threads,
        "unify_dictionaries": resolved.unify_dictionaries,
        "emit_dictionary_deltas": resolved.emit_dictionary_deltas,
        "allow_64bit": resolved.allow_64bit,
        "metadata_version": str(resolved.metadata_version),
    }


def read_table_ipc_file(path: PathLike) -> TableLike:
    """Read an Arrow IPC file.

    Returns
    -------
    TableLike
        Loaded table.
    """
    target = ensure_path(path)
    options = ipc_read_options_factory()
    with pa.memory_map(str(target), "r") as source:
        reader = pa_ipc.open_file(source, options=options)
        return reader.read_all()


def read_table_ipc_stream(path: PathLike) -> TableLike:
    """Read an Arrow IPC stream.

    Returns
    -------
    TableLike
        Loaded table.
    """
    target = ensure_path(path)
    options = ipc_read_options_factory()
    with pa.memory_map(str(target), "r") as source:
        reader = pa_ipc.open_stream(source, options=options)
        return reader.read_all()


__all__ = [
    "ipc_write_config_payload",
    "read_table_ipc_file",
    "read_table_ipc_stream",
    "write_ipc_bundle",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
