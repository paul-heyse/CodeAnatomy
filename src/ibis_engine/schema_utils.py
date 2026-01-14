"""Schema helpers for Ibis expressions."""

from __future__ import annotations

from collections.abc import Sequence

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Table, Value


def ibis_dtype_from_arrow(dtype: pa.DataType) -> dt.DataType:
    """Return the Ibis dtype corresponding to a PyArrow dtype.

    Returns
    -------
    ibis.expr.datatypes.DataType
        Ibis dtype for the provided Arrow dtype.
    """
    try:
        return ibis.dtype(dtype)
    except (TypeError, ValueError):
        return ibis.dtype(str(dtype))


def ibis_null_literal(dtype: pa.DataType) -> Value:
    """Return a typed null literal for a PyArrow dtype.

    Returns
    -------
    ibis.expr.types.Value
        Typed null literal expression.
    """
    ibis_type = ibis_dtype_from_arrow(dtype)
    return ibis.literal(None, type=ibis_type)


def align_table_to_schema(
    table: Table,
    *,
    schema: pa.Schema,
    keep_extra_columns: bool = False,
) -> Table:
    """Align an Ibis table to a target schema via projection and casts.

    Returns
    -------
    ibis.expr.types.Table
        Table aligned to the target schema.
    """
    cols: list[Value] = []
    seen = set()
    for field in schema:
        name = field.name
        if name in table.columns:
            cols.append(table[name].cast(ibis_dtype_from_arrow(field.type)).name(name))
        else:
            cols.append(ibis_null_literal(field.type).name(name))
        seen.add(name)
    if keep_extra_columns:
        cols.extend(table[name] for name in table.columns if name not in seen)
    return table.select(cols)


def ensure_columns(
    table: Table,
    *,
    schema: pa.Schema,
    only_missing: bool = True,
) -> Table:
    """Ensure table has columns for each schema field, filling missing with nulls.

    Returns
    -------
    ibis.expr.types.Table
        Table with all schema fields present.
    """
    expr = table
    for field in schema:
        name = field.name
        if only_missing and name in expr.columns:
            continue
        expr = expr.mutate(**{name: ibis_null_literal(field.type)})
    return expr


def coalesce_columns(
    table: Table,
    columns: Sequence[str],
    *,
    default: Value | None = None,
) -> Value:
    """Return a coalesced expression across the named columns.

    Returns
    -------
    ibis.expr.types.Value
        Coalesced expression for the provided column names.
    """
    values: list[Value] = [table[name] for name in columns if name in table.columns]
    if not values:
        if default is None:
            return ibis.null()
        return default
    if default is not None:
        values.append(default)
    return ibis.coalesce(*values)


__all__ = [
    "align_table_to_schema",
    "coalesce_columns",
    "ensure_columns",
    "ibis_dtype_from_arrow",
    "ibis_null_literal",
]
