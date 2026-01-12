"""Shared hashing helpers for extractors."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import arrowdsl.core.interop as pa
from arrowdsl.core.ids import HashSpec, hash_column_values
from arrowdsl.core.interop import ArrayLike, TableLike, pc
from arrowdsl.schema.arrays import set_or_append_column


def valid_mask(table: TableLike, cols: Sequence[str]) -> ArrayLike:
    """Return a validity mask for the provided columns.

    Returns
    -------
    ArrayLike
        Boolean mask of rows with all columns valid.
    """
    mask = pc.is_valid(table[cols[0]])
    for col in cols[1:]:
        mask = pc.and_(mask, pc.is_valid(table[col]))
    return mask


def apply_hash_column(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> TableLike:
    """Hash columns into a new id column.

    Returns
    -------
    TableLike
        Updated table with hashed id column.
    """
    hashed = hash_column_values(table, spec=spec)
    out_col = spec.out_col or f"{spec.prefix}_id"
    if required:
        mask = valid_mask(table, required)
        hashed = pc.if_else(mask, hashed, pa.scalar(None, type=hashed.type))
    return set_or_append_column(table, out_col, hashed)


def apply_hash_columns(
    table: TableLike,
    *,
    specs: Sequence[HashSpec],
    required: Mapping[str, Sequence[str]] | None = None,
) -> TableLike:
    """Apply multiple hash specs to a table.

    Returns
    -------
    TableLike
        Updated table with hashed id columns.
    """
    out = table
    for spec in specs:
        out_col = spec.out_col or f"{spec.prefix}_id"
        req = required.get(out_col) if required else None
        out = apply_hash_column(out, spec=spec, required=req)
    return out
