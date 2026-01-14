"""Arrow IPC read/write helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.io import IpcWriteConfig, ipc_read_options_factory, ipc_write_options_factory
from core_types import PathLike, ensure_path


def _ensure_dir(path: Path) -> None:
    """Ensure the directory exists for a target path.

    Parameters
    ----------
    path
        Directory path to create if missing.
    """
    path.mkdir(exist_ok=True, parents=True)


def write_table_ipc_file(
    table: TableLike,
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

    schema = table.schema
    if schema_metadata:
        schema = SchemaMetadataSpec(schema_metadata=schema_metadata).apply(schema)
        table = table.cast(schema, safe=False)

    options = ipc_write_options_factory(config)
    with (
        pa.OSFile(str(target), "wb") as sink,
        pa_ipc.new_file(sink, schema, options=options) as writer,
    ):
        writer.write_table(table)
    return str(target)


def write_table_ipc_stream(
    table: TableLike,
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

    schema = table.schema
    if schema_metadata:
        schema = SchemaMetadataSpec(schema_metadata=schema_metadata).apply(schema)
        table = table.cast(schema, safe=False)

    options = ipc_write_options_factory(config)
    with (
        pa.OSFile(str(target), "wb") as sink,
        pa_ipc.new_stream(sink, schema, options=options) as writer,
    ):
        writer.write_table(table)
    return str(target)


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


__all__ = [
    "read_table_ipc_file",
    "write_table_ipc_file",
    "write_table_ipc_stream",
]
