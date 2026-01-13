"""Spec table IO helpers."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pyarrow import ipc

from arrowdsl.core.interop import SchemaLike

if TYPE_CHECKING:
    from arrowdsl.spec.tables.base import SpecTableCodec


def write_spec_table(path: str | Path, table: pa.Table) -> None:
    """Write a spec table to an IPC file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with ipc.new_file(str(target), table.schema) as writer:
        writer.write_table(table)


def read_spec_table(path: str | Path) -> pa.Table:
    """Read a spec table from an IPC file.

    Returns
    -------
    pa.Table
        Table loaded from disk.
    """
    with ipc.open_file(str(path)) as reader:
        return reader.read_all()


def write_spec_values[SpecT](
    codec: SpecTableCodec[SpecT],
    path: str | Path,
    values: Sequence[SpecT],
) -> None:
    """Write spec values to an IPC file."""
    write_spec_table(path, codec.to_table(values))


def read_spec_values[SpecT](
    codec: SpecTableCodec[SpecT],
    path: str | Path,
) -> tuple[SpecT, ...]:
    """Read spec values from an IPC file.

    Returns
    -------
    tuple[SpecT, ...]
        Spec values decoded from the table.
    """
    return codec.from_table(read_spec_table(path))


def table_from_json(
    schema: SchemaLike,
    payload: list[dict[str, Any]],
) -> pa.Table:
    """Build a spec table from JSON records.

    Returns
    -------
    pa.Table
        Table built from JSON payload.
    """
    return pa.Table.from_pylist(payload, schema=schema)


def table_from_json_file(schema: SchemaLike, path: str | Path) -> pa.Table:
    """Build a spec table from a JSON file.

    Returns
    -------
    pa.Table
        Table built from JSON payload.

    Raises
    ------
    TypeError
        Raised when the JSON payload is not a list of objects.
    """
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        msg = "Spec JSON payload must be a list of objects."
        raise TypeError(msg)
    return table_from_json(schema, data)


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
    "read_spec_table",
    "read_spec_values",
    "sort_spec_table",
    "table_from_json",
    "table_from_json_file",
    "write_spec_table",
    "write_spec_values",
]
