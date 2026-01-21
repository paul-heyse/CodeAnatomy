"""Schema introspection helpers for DataFusion sessions."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.schema_constants import DEFAULT_VALUE_META

_TABLE_DEFINITION_OVERRIDES: dict[int, dict[str, str]] = {}


def record_table_definition_override(
    ctx: SessionContext,
    *,
    name: str,
    ddl: str,
) -> None:
    """Record a CREATE TABLE definition override for a session.

    Parameters
    ----------
    ctx
        DataFusion session context to scope the override.
    name
        Table name associated with the override.
    ddl
        CREATE TABLE statement to record for the table.
    """
    overrides = _TABLE_DEFINITION_OVERRIDES.setdefault(id(ctx), {})
    overrides[name] = ddl


def table_definition_override(ctx: SessionContext, *, name: str) -> str | None:
    """Return a recorded table definition override when available.

    Parameters
    ----------
    ctx
        DataFusion session context that scoped the override.
    name
        Table name to look up.

    Returns
    -------
    str | None
        Recorded CREATE TABLE statement when available.
    """
    return _TABLE_DEFINITION_OVERRIDES.get(id(ctx), {}).get(name)


def _rows_for_query(ctx: SessionContext, query: str) -> list[dict[str, object]]:
    """Return a list of row mappings for a SQL query.

    Returns
    -------
    list[dict[str, object]]
        Rows represented as dictionaries keyed by column name.
    """
    table = ctx.sql(query).to_arrow_table()
    return [dict(row) for row in table.to_pylist()]


def _defaults_from_schema(schema: pa.Schema) -> dict[str, object]:
    defaults: dict[str, object] = {}

    def _walk_field(field: pa.Field, *, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        metadata = field.metadata or {}
        default = metadata.get(DEFAULT_VALUE_META)
        if default is not None:
            defaults[path] = default.decode("utf-8", errors="replace")
        _walk_dtype(field.type, prefix=path)

    def _walk_dtype(dtype: pa.DataType, *, prefix: str) -> None:
        if pa.types.is_struct(dtype):
            for child in dtype:
                _walk_field(child, prefix=prefix)
            return
        if (
            pa.types.is_list(dtype)
            or pa.types.is_large_list(dtype)
            or pa.types.is_list_view(dtype)
            or pa.types.is_large_list_view(dtype)
        ):
            _walk_dtype(dtype.value_type, prefix=prefix)
            return
        if pa.types.is_map(dtype):
            _walk_dtype(dtype.item_type, prefix=prefix)

    for field in schema:
        _walk_field(field, prefix="")
    return defaults


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
            "SELECT table_catalog, table_schema, table_name, column_name, data_type, "
            "is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_name = '{table_name}'"
        )
        try:
            return _rows_for_query(self.ctx, query)
        except (RuntimeError, TypeError, ValueError):
            fallback = (
                "SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                f"WHERE table_name = '{table_name}'"
            )
            return _rows_for_query(self.ctx, fallback)

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

    def catalogs_snapshot(self) -> list[dict[str, object]]:
        """Return catalog inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Catalog inventory rows.
        """
        query = "SELECT DISTINCT catalog_name FROM information_schema.schemata"
        return _rows_for_query(self.ctx, query)

    def schemata_snapshot(self) -> list[dict[str, object]]:
        """Return schema inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Schema inventory rows including catalog and schema names.
        """
        query = "SELECT catalog_name, schema_name FROM information_schema.schemata"
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

    def routines_snapshot(self) -> list[dict[str, object]]:
        """Return routine inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Routine inventory rows including name and type.
        """
        query = (
            "SELECT routine_catalog, routine_schema, routine_name, routine_type "
            "FROM information_schema.routines"
        )
        return _rows_for_query(self.ctx, query)

    def parameters_snapshot(self) -> list[dict[str, object]]:
        """Return routine parameter rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Parameter metadata rows including names and data types.
        """
        query = (
            "SELECT specific_name, routine_name, parameter_name, parameter_mode, "
            "data_type, ordinal_position "
            "FROM information_schema.parameters"
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

    def table_column_defaults(self, table_name: str) -> dict[str, object]:
        """Return column default metadata for a table when available.

        Returns
        -------
        dict[str, object]
            Mapping of column names to default expressions.
        """
        rows = self.table_columns(table_name)
        defaults: dict[str, object] = {}
        for row in rows:
            name = row.get("column_name")
            default = row.get("column_default")
            if name is None or default is None:
                continue
            defaults[str(name)] = default
        if defaults:
            return defaults
        schema = self._arrow_schema_for_table(table_name)
        if schema is None:
            return defaults
        return _defaults_from_schema(schema)

    def _arrow_schema_for_table(self, table_name: str) -> pa.Schema | None:
        try:
            df = self.ctx.table(table_name)
        except (KeyError, RuntimeError, TypeError, ValueError):
            return None
        schema = df.schema()
        if isinstance(schema, pa.Schema):
            return schema
        to_arrow = getattr(schema, "to_arrow", None)
        if callable(to_arrow):
            resolved = to_arrow()
            if isinstance(resolved, pa.Schema):
                return resolved
        return None

    def table_logical_plan(self, table_name: str) -> str | None:
        """Return a logical plan description for a table when available.

        Returns
        -------
        str | None
            Logical plan description when available.
        """
        try:
            df = self.ctx.table(table_name)
        except (KeyError, RuntimeError, TypeError, ValueError):
            return None
        try:
            plan = df.logical_plan()
        except (RuntimeError, TypeError, ValueError):
            return None
        return str(plan)

    def table_definition(self, table_name: str) -> str | None:
        """Return a CREATE TABLE definition when supported.

        Parameters
        ----------
        table_name
            Table name to describe.

        Returns
        -------
        str | None
            CREATE TABLE statement when available.
        """
        try:
            rows = _rows_for_query(self.ctx, f"SHOW CREATE TABLE {table_name}")
        except (RuntimeError, TypeError, ValueError):
            return table_definition_override(self.ctx, name=table_name)
        if not rows:
            return table_definition_override(self.ctx, name=table_name)
        first = rows[0]
        if len(first) == 1:
            return str(next(iter(first.values())))
        return repr(first)

    def table_constraints(self, table_name: str) -> tuple[str, ...]:
        """Return constraint expressions for a table when available.

        Parameters
        ----------
        table_name
            Table name to inspect.

        Returns
        -------
        tuple[str, ...]
            Constraint expressions or identifiers.
        """
        try:
            rows = _rows_for_query(
                self.ctx,
                "SELECT constraint_name, constraint_type, constraint_definition "
                "FROM information_schema.table_constraints "
                f"WHERE table_name = '{table_name}'",
            )
        except (RuntimeError, TypeError, ValueError):
            return ()
        constraints: list[str] = []
        for row in rows:
            definition = row.get("constraint_definition")
            name = row.get("constraint_name")
            if definition:
                constraints.append(str(definition))
            elif name:
                constraints.append(str(name))
        return tuple(constraints)


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


__all__ = [
    "SchemaIntrospector",
    "find_struct_field_keys",
    "record_table_definition_override",
    "table_definition_override",
]
