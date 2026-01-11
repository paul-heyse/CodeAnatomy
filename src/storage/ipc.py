"""Arrow IPC read/write helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from core_types import PathLike, ensure_path


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


def write_table_ipc_file(table: pa.Table, path: PathLike, *, overwrite: bool = True) -> str:
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

    with pa.OSFile(str(target), "wb") as sink, pa_ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return str(target)


def write_table_ipc_stream(table: pa.Table, path: PathLike, *, overwrite: bool = True) -> str:
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

    with pa.OSFile(str(target), "wb") as sink, pa_ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return str(target)


def read_table_ipc_file(path: PathLike) -> pa.Table:
    """Read an Arrow IPC file.

    Returns
    -------
    pa.Table
        Loaded table.
    """
    target = ensure_path(path)
    with pa.memory_map(str(target), "r") as source:
        reader = pa_ipc.open_file(source)
        return reader.read_all()
