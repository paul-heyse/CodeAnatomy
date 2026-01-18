"""Schema helpers for Ibis expressions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Table, Value
from sqlglot import Expression


def ibis_dtype_from_arrow(dtype: pa.DataType) -> dt.DataType:
    """Return the Ibis dtype corresponding to a PyArrow dtype.

    Returns
    -------
    ibis.expr.datatypes.DataType
        Ibis dtype for the provided Arrow dtype.
    """
    normalized = _normalize_arrow_dtype(dtype)
    try:
        return ibis.dtype(normalized)
    except (TypeError, ValueError):
        return ibis.dtype(str(normalized))


def ibis_null_literal(dtype: pa.DataType) -> Value:
    """Return a typed null literal for a PyArrow dtype.

    Returns
    -------
    ibis.expr.types.Value
        Typed null literal expression.
    """
    ibis_type = ibis_dtype_from_arrow(dtype)
    return ibis.literal(None, type=ibis_type)


def ibis_schema_from_arrow(schema: pa.Schema) -> ibis.Schema:
    """Return an Ibis schema for a PyArrow schema.

    Returns
    -------
    ibis.Schema
        Ibis schema derived from the Arrow schema.
    """
    fields = {field.name: ibis_dtype_from_arrow(field.type) for field in schema}
    return ibis.schema(fields)


def sqlglot_column_defs(schema: pa.Schema, *, dialect: str = "datafusion") -> list[Expression]:
    """Return SQLGlot column defs for an Arrow schema.

    Returns
    -------
    list[sqlglot.Expression]
        SQLGlot column definition expressions.
    """
    ibis_schema = ibis_schema_from_arrow(schema)
    return list(ibis_schema.to_sqlglot_column_defs(dialect=dialect))


def validate_expr_schema(expr: Table, *, expected: pa.Schema) -> None:
    """Validate that an Ibis expression matches the expected schema.

    Raises
    ------
    ValueError
        Raised when the expression schema differs from the expected schema.
    """
    expected_ibis = ibis_schema_from_arrow(expected)
    actual = expr.schema()
    expected_names = cast("tuple[str, ...]", expected_ibis.names)
    actual_names = cast("tuple[str, ...]", actual.names)
    missing = [name for name in expected_names if name not in actual_names]
    extra = [name for name in actual_names if name not in expected_names]
    mismatched = [
        name
        for name in expected_names
        if name in actual_names and actual[name] != expected_ibis[name]
    ]
    if not missing and not extra and not mismatched:
        return
    details: list[str] = []
    if missing:
        details.append(f"missing={missing}")
    if extra:
        details.append(f"extra={extra}")
    if mismatched:
        details.append(f"mismatched={mismatched}")
    msg = "Ibis expression schema does not match expected contract."
    msg_full = "{} ({})".format(msg, ", ".join(details))
    raise ValueError(msg_full)


def normalize_table_for_ibis(table: pa.Table) -> pa.Table:
    """Return a table with Arrow view types normalized for Ibis.

    Returns
    -------
    pyarrow.Table
        Table with list view types normalized to list types.
    """
    normalized_schema = _normalize_arrow_schema(table.schema)
    if normalized_schema == table.schema:
        return table
    return pa.table(table.to_pydict(), schema=normalized_schema)


def _normalize_arrow_dtype(dtype: pa.DataType) -> pa.DataType:
    if pa.types.is_list_view(dtype):
        normalized = pa.list_(_normalize_arrow_dtype(dtype.value_type))
    elif pa.types.is_large_list_view(dtype):
        normalized = pa.large_list(_normalize_arrow_dtype(dtype.value_type))
    elif pa.types.is_fixed_size_list(dtype):
        normalized = pa.list_(_normalize_arrow_dtype(dtype.value_type), dtype.list_size)
    elif pa.types.is_list(dtype):
        normalized = pa.list_(_normalize_arrow_dtype(dtype.value_type))
    elif pa.types.is_large_list(dtype):
        normalized = pa.large_list(_normalize_arrow_dtype(dtype.value_type))
    elif pa.types.is_struct(dtype):
        fields = [
            pa.field(
                field.name,
                _normalize_arrow_dtype(field.type),
                nullable=field.nullable,
                metadata=field.metadata,
            )
            for field in dtype
        ]
        normalized = pa.struct(fields)
    elif pa.types.is_map(dtype):
        normalized = pa.map_(
            _normalize_arrow_dtype(dtype.key_type),
            _normalize_arrow_dtype(dtype.item_type),
            keys_sorted=dtype.keys_sorted,
        )
    elif pa.types.is_dictionary(dtype):
        normalized = pa.dictionary(dtype.index_type, _normalize_arrow_dtype(dtype.value_type))
    else:
        normalized = dtype
    return normalized


def _normalize_arrow_schema(schema: pa.Schema) -> pa.Schema:
    fields = [
        pa.field(
            field.name,
            _normalize_arrow_dtype(field.type),
            nullable=field.nullable,
            metadata=field.metadata,
        )
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


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
    "ibis_schema_from_arrow",
    "normalize_table_for_ibis",
    "sqlglot_column_defs",
    "validate_expr_schema",
]
