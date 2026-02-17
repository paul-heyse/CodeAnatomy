"""Introspection helper functions extracted from introspection_core."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.arrow.interop import coerce_arrow_schema
from datafusion_engine.schema.introspection_core import SchemaIntrospector


def parameters_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table | None:
    """Return information_schema.parameters as a pyarrow.Table when available.

    Returns:
    -------
    pyarrow.Table | None
        Parameter inventory from information_schema.parameters, or None if unavailable.
    """
    from datafusion_engine.schema.introspection_routines import (
        parameters_snapshot_table as _parameters_snapshot_table,
    )

    return _parameters_snapshot_table(ctx, sql_options=sql_options)


def table_constraint_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return constraint metadata rows for a table when available.

    Returns:
    -------
    list[dict[str, object]]
        Rows including constraint type and column names where available.
    """
    from datafusion_engine.schema.introspection_delta import (
        table_constraint_rows as _table_constraint_rows,
    )

    return _table_constraint_rows(
        ctx,
        table_name=table_name,
        sql_options=sql_options,
    )


def constraint_rows(
    ctx: SessionContext,
    *,
    catalog: str | None = None,
    schema: str | None = None,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return constraint metadata rows across tables.

    Returns:
    -------
    list[dict[str, object]]
        Rows including constraint type and column names where available.
    """
    from datafusion_engine.schema.introspection_delta import constraint_rows as _constraint_rows

    return _constraint_rows(
        ctx,
        catalog=catalog,
        schema=schema,
        sql_options=sql_options,
    )


def schema_from_table(ctx: SessionContext, name: str) -> pa.Schema:
    """Return Arrow schema from DataFusion catalog for a table.

    Args:
        ctx: Description.
        name: Description.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    df = ctx.table(name)
    schema = coerce_arrow_schema(df.schema())
    if schema is not None:
        return schema
    msg = "Unable to resolve DataFusion schema to Arrow schema."
    raise TypeError(msg)


def _table_name_from_ddl(ddl: str) -> str:
    """Extract table name from CREATE EXTERNAL TABLE DDL.

    Args:
        ddl: Description.

    Returns:
        str: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    lines = ddl.split("\n")
    for line in lines:
        if line.strip().upper().startswith("CREATE EXTERNAL TABLE"):
            parts = line.split()
            for i, part in enumerate(parts):
                if part.upper() == "TABLE" and i + 1 < len(parts):
                    table_name = parts[i + 1].strip()
                    if table_name.endswith("("):
                        table_name = table_name[:-1].strip()
                    return table_name
    msg = f"Could not extract table name from DDL: {ddl[:100]}"
    raise ValueError(msg)


def find_struct_field_keys(
    schema: pa.Schema,
    *,
    field_names: Sequence[str],
) -> tuple[str, ...]:
    """Return struct field keys for the first matching nested field.

    Args:
        schema: Description.
        field_names: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    for schema_field in schema:
        keys = _find_struct_keys_in_type(schema_field.type, field_names=field_names)
        if keys is not None:
            return keys
    msg = f"Schema missing struct fields for {tuple(field_names)!r}."
    raise KeyError(msg)


def _find_struct_keys_in_type(
    dtype: pa.DataType,
    *,
    field_names: Iterable[str],
) -> tuple[str, ...] | None:
    if pa.types.is_struct(dtype):
        result: tuple[str, ...] | None = None
        for struct_field in dtype:
            if struct_field.name in field_names:
                result = _extract_struct_keys(struct_field.type)
                break
            result = _find_struct_keys_in_type(struct_field.type, field_names=field_names)
            if result is not None:
                break
        return result
    if (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_list_view(dtype)
        or pa.types.is_large_list_view(dtype)
    ):
        return _find_struct_keys_in_type(dtype.value_type, field_names=field_names)
    if pa.types.is_map(dtype):
        return _find_struct_keys_in_type(dtype.item_type, field_names=field_names)
    return None


def _extract_struct_keys(dtype: pa.DataType) -> tuple[str, ...] | None:
    if pa.types.is_struct(dtype):
        return tuple(struct_field.name for struct_field in dtype)
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return _extract_struct_keys(dtype.value_type)
    if pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype):
        return _extract_struct_keys(dtype.value_type)
    return None


def _function_catalog_sort_key(row: Mapping[str, object]) -> tuple[str, str]:
    name = row.get("function_name")
    func_name = str(name) if name is not None else ""
    func_type = row.get("function_type")
    return func_name, str(func_type) if func_type is not None else ""


def catalogs_snapshot(introspector: SchemaIntrospector) -> list[dict[str, object]]:
    """Return catalog inventory rows from information_schema.

    Returns:
    -------
    list[dict[str, object]]
        Catalog inventory rows.
    """
    from datafusion_engine.schema.introspection_cache import catalogs_snapshot as _catalogs_snapshot

    return _catalogs_snapshot(introspector)
