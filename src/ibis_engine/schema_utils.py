"""Schema helpers for Ibis expressions.

Deprecation notice:
Schema normalization and drift resolution should now be handled at scan-time
via DataFusion physical expression adapters rather than downstream Ibis
transforms. When `enable_schema_evolution_adapter=True` in the runtime profile,
schema adapters normalize batches at the TableProvider boundary, eliminating
the need for explicit cast/projection operations in Ibis transforms.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.interop import SchemaLike
from sqlglot_tools.compat import Expression


def _normalize_arrow_dtype(dtype: pa.DataType) -> pa.DataType:
    """Normalize Arrow types for Ibis compatibility.

    Parameters
    ----------
    dtype:
        Arrow dtype to normalize for Ibis conversions.

    Returns
    -------
    pyarrow.DataType
        Normalized Arrow dtype.
    """
    if pa.types.is_dictionary(dtype):
        return dtype.value_type
    if pa.types.is_large_string(dtype):
        return pa.string()
    if pa.types.is_large_binary(dtype):
        return pa.binary()
    if pa.types.is_large_list(dtype):
        return pa.list_(dtype.value_type)
    return dtype


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


def resolve_arrow_schema(schema: SchemaLike) -> pa.Schema:
    """Return a PyArrow schema for a schema-like input.

    Returns
    -------
    pyarrow.Schema
        Resolved schema instance.

    Raises
    ------
    TypeError
        Raised when the schema cannot be resolved.
    """
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Schema-like object could not be resolved to a pyarrow.Schema."
    raise TypeError(msg)


def sqlglot_column_defs(schema: pa.Schema, *, dialect: str = "datafusion") -> list[Expression]:
    """Return SQLGlot column defs for an Arrow schema.

    Returns
    -------
    list[sqlglot.Expression]
        SQLGlot column definition expressions.
    """
    ibis_schema = ibis_schema_from_arrow(schema)
    return list(ibis_schema.to_sqlglot_column_defs(dialect=dialect))


def sqlglot_column_sql(schema: pa.Schema, *, dialect: str = "datafusion") -> list[str]:
    """Return SQL-rendered column definitions for an Arrow schema.

    Returns
    -------
    list[str]
        Column definition SQL strings.
    """
    return [col.sql(dialect=dialect) for col in sqlglot_column_defs(schema, dialect=dialect)]


def validate_expr_schema(
    expr: Table,
    *,
    expected: pa.Schema,
    allow_extra_columns: bool = False,
) -> None:
    """Validate that an Ibis expression matches the expected schema.

    Parameters
    ----------
    expr:
        Expression to validate against the expected schema.
    expected:
        Schema expected for the expression result.
    allow_extra_columns:
        Whether to allow columns not present in the expected schema.

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
    extra: list[str] = []
    if not allow_extra_columns:
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


def bind_expr_schema(
    expr: Table,
    *,
    schema: SchemaLike,
    allow_extra_columns: bool = False,
) -> Table:
    """Bind an Ibis expression to an explicit schema.

    Parameters
    ----------
    expr:
        Expression to bind.
    schema:
        Expected schema for the expression.
    allow_extra_columns:
        Whether to preserve columns not present in the schema.

    Returns
    -------
    ibis.expr.types.Table
        Expression cast and ordered according to the schema.

    Raises
    ------
    ValueError
        Raised when required columns are missing.
    """
    arrow_schema = resolve_arrow_schema(schema)
    expected = ibis_schema_from_arrow(arrow_schema)
    expected_names = tuple(arrow_schema.names)
    expr_names = tuple(expr.columns)
    missing = [name for name in expected_names if name not in expr_names]
    if missing:
        msg = f"Ibis expression is missing required columns: {missing}."
        raise ValueError(msg)
    columns: list[Value] = []
    for field in arrow_schema:
        col = expr[field.name].cast(expected[field.name]).name(field.name)
        columns.append(col)
    if allow_extra_columns:
        extras = [name for name in expr_names if name not in expected_names]
        columns.extend([expr[name] for name in extras])
    return expr.select(columns)


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
    "bind_expr_schema",
    "coalesce_columns",
    "ensure_columns",
    "ibis_dtype_from_arrow",
    "ibis_null_literal",
    "ibis_schema_from_arrow",
    "resolve_arrow_schema",
    "sqlglot_column_defs",
    "sqlglot_column_sql",
    "validate_expr_schema",
]
