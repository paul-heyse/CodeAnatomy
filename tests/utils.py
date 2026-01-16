"""Test helpers for Arrow table assertions."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.types as patypes

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike

SortKey = tuple[str, str]


def table_digest(
    value: TableLike | RecordBatchReaderLike,
    *,
    sort_keys: Sequence[SortKey] | None = None,
) -> str:
    """Return a stable digest for a table-like value.

    Parameters
    ----------
    value:
        Table or reader to hash.
    sort_keys:
        Optional sort keys used to canonicalize row order.

    Returns
    -------
    str
        SHA-256 digest for the table rows.
    """
    table = _ensure_table(value)
    if sort_keys:
        _validate_sort_keys(table.schema.names, sort_keys)
        table = _coerce_sort_columns(table, sort_keys=sort_keys)
        table = table.sort_by(list(sort_keys))
    hasher = hashlib.sha256()
    for column in table.columns:
        hasher.update(repr(column.to_pylist()).encode("utf-8"))
    return hasher.hexdigest()


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> pa.Table:
    table = value.read_all() if isinstance(value, RecordBatchReaderLike) else value
    if isinstance(table, pa.Table):
        return table
    return pa.Table.from_pylist(table.to_pylist())


def _validate_sort_keys(column_names: Sequence[str], sort_keys: Sequence[SortKey]) -> None:
    missing = [name for name, _ in sort_keys if name not in column_names]
    if not missing:
        return
    msg = f"Sort keys missing from table schema: {missing}"
    raise ValueError(msg)


def _coerce_sort_columns(table: pa.Table, *, sort_keys: Sequence[SortKey]) -> pa.Table:
    sort_names = {name for name, _ in sort_keys}
    columns = []
    names = []
    for field in table.schema:
        col = table[field.name]
        if field.name in sort_names and patypes.is_dictionary(field.type):
            col = pc.cast(col, field.type.value_type, safe=False)
        columns.append(col)
        names.append(field.name)
    return pa.table(columns, names=names)
