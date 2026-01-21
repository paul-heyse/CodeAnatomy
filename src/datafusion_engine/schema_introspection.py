"""Schema introspection helpers for DataFusion sessions."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext


def _rows_for_query(ctx: SessionContext, query: str) -> list[dict[str, object]]:
    """Return a list of row mappings for a SQL query.

    Returns
    -------
    list[dict[str, object]]
        Rows represented as dictionaries keyed by column name.
    """
    table = ctx.sql(query).to_arrow_table()
    return [dict(row) for row in table.to_pylist()]


@dataclass(frozen=True)
class SchemaIntrospector:
    """Expose schema reflection across tables, queries, and settings."""

    ctx: SessionContext

    def describe_query(self, sql: str) -> list[dict[str, object]]:
        """Return the computed output schema for a SQL query.

        Returns
        -------
        list[dict[str, object]]
            ``DESCRIBE`` rows for the query.
        """
        return _rows_for_query(self.ctx, f"DESCRIBE {sql}")

    def table_columns(self, table_name: str) -> list[dict[str, object]]:
        """Return column metadata from information_schema for a table.

        Returns
        -------
        list[dict[str, object]]
            Column metadata rows for the table.
        """
        query = (
            "SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_name = '{table_name}'"
        )
        return _rows_for_query(self.ctx, query)

    def tables_snapshot(self) -> list[dict[str, object]]:
        """Return table inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Table inventory rows including catalog/schema/type.
        """
        query = (
            "SELECT table_catalog, table_schema, table_name, table_type "
            "FROM information_schema.tables"
        )
        return _rows_for_query(self.ctx, query)

    def columns_snapshot(self) -> list[dict[str, object]]:
        """Return all column metadata rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Column metadata rows for all tables.
        """
        query = (
            "SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable "
            "FROM information_schema.columns"
        )
        return _rows_for_query(self.ctx, query)

    def settings_snapshot(self) -> list[dict[str, object]]:
        """Return session settings from information_schema.df_settings.

        Returns
        -------
        list[dict[str, object]]
            Session settings rows with name/value pairs.
        """
        query = "SELECT name, value FROM information_schema.df_settings"
        return _rows_for_query(self.ctx, query)


def find_struct_field_keys(
    schema: pa.Schema,
    *,
    field_names: Sequence[str],
) -> tuple[str, ...]:
    """Return struct field keys for the first matching nested field.

    Returns
    -------
    tuple[str, ...]
        Struct field names for the matched nested field.

    Raises
    ------
    KeyError
        Raised when no matching struct field is found in the schema.
    """
    for field in schema:
        keys = _find_struct_keys_in_type(field.type, field_names=field_names)
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
        for field in dtype:
            if field.name in field_names:
                result = _extract_struct_keys(field.type)
                break
            result = _find_struct_keys_in_type(field.type, field_names=field_names)
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
        return tuple(field.name for field in dtype)
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return _extract_struct_keys(dtype.value_type)
    if pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype):
        return _extract_struct_keys(dtype.value_type)
    return None


__all__ = ["SchemaIntrospector", "find_struct_field_keys"]
