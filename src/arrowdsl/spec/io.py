"""Spec table IO helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pyarrow import ipc

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.build import (
    rows_from_table as build_rows_from_table,
)
from arrowdsl.schema.build import (
    table_from_rows as build_table_from_rows,
)
from storage.ipc import IpcWriteConfig, ipc_read_options_factory, ipc_write_options_factory

if TYPE_CHECKING:
    from arrowdsl.spec.tables.base import SpecTableCodec


def write_spec_table(
    path: str | Path,
    table: pa.Table,
    *,
    options: ipc.IpcWriteOptions | None = None,
) -> None:
    """Write a spec table to an IPC file."""
    options = options or ipc_write_options_factory()
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with ipc.new_file(str(target), table.schema, options=options) as writer:
        writer.write_table(table)


def read_spec_table(
    path: str | Path,
    *,
    options: ipc.IpcReadOptions | None = None,
) -> pa.Table:
    """Read a spec table from an IPC file.

    Returns
    -------
    pa.Table
        Table loaded from disk.
    """
    options = options or ipc_read_options_factory()
    with ipc.open_file(str(path), options=options) as reader:
        return reader.read_all()


def write_spec_values[SpecT](
    codec: SpecTableCodec[SpecT],
    path: str | Path,
    values: Sequence[SpecT],
    *,
    options: ipc.IpcWriteOptions | None = None,
) -> None:
    """Write spec values to an IPC file."""
    write_spec_table(path, codec.to_table(values), options=options)


def read_spec_values[SpecT](
    codec: SpecTableCodec[SpecT],
    path: str | Path,
    *,
    options: ipc.IpcReadOptions | None = None,
) -> tuple[SpecT, ...]:
    """Read spec values from an IPC file.

    Returns
    -------
    tuple[SpecT, ...]
        Spec values decoded from the table.
    """
    return codec.from_table(read_spec_table(path, options=options))


def table_from_rows(
    schema: SchemaLike,
    rows: Iterable[Mapping[str, Any]],
) -> pa.Table:
    """Build a spec table from row dictionaries.

    Returns
    -------
    pa.Table
        Table built from rows with the provided schema.
    """
    return build_table_from_rows(schema, rows)


def rows_from_table(table: pa.Table) -> list[dict[str, Any]]:
    """Return rows from a spec table.

    Returns
    -------
    list[dict[str, Any]]
        Row dictionaries from the table.
    """
    return build_rows_from_table(table)


def sort_spec_table(table: pa.Table, *, keys: Sequence[str]) -> pa.Table:
    """Return a deterministically sorted copy of a spec table.

    Returns
    -------
    pa.Table
        Sorted table copy.

    Raises
    ------
    ValueError
        Raised when required sort keys are missing.
    """
    if not keys:
        return table
    missing = [key for key in keys if key not in table.column_names]
    if missing:
        msg = f"Spec table is missing sort keys: {missing}."
        raise ValueError(msg)
    sort_keys = [(key, "ascending") for key in keys]
    return table.sort_by(sort_keys)


__all__ = [
    "IpcWriteConfig",
    "ipc_read_options_factory",
    "ipc_write_options_factory",
    "read_spec_table",
    "read_spec_values",
    "rows_from_table",
    "sort_spec_table",
    "table_from_rows",
    "write_spec_table",
    "write_spec_values",
]
