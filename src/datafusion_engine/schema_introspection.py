"""Schema introspection helpers for DataFusion sessions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext

from registry_common.arrow_payloads import payload_hash

_TABLE_DEFINITION_OVERRIDES: dict[int, dict[str, str]] = {}
SCHEMA_MAP_HASH_VERSION: int = 1

_SCHEMA_MAP_COLUMN_SCHEMA = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("dtype", pa.string()),
    ]
)
_SCHEMA_MAP_ENTRY_SCHEMA = pa.struct(
    [
        pa.field("table", pa.string()),
        pa.field("columns", pa.list_(_SCHEMA_MAP_COLUMN_SCHEMA)),
    ]
)
_SCHEMA_MAP_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_SCHEMA_MAP_ENTRY_SCHEMA)),
    ]
)


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
        return _rows_for_query(self.ctx, query)

    def table_columns_with_ordinal(self, table_name: str) -> list[dict[str, object]]:
        """Return ordered column metadata rows for a table.

        Returns
        -------
        list[dict[str, object]]
            Column metadata rows ordered by ordinal position.
        """
        query = (
            "SELECT table_catalog, table_schema, table_name, column_name, data_type, "
            "ordinal_position, is_nullable, column_default "
            "FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' "
            "ORDER BY ordinal_position"
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

    def tables_snapshot_table(self) -> pa.Table:
        """Return table inventory rows as a pyarrow.Table.

        Returns
        -------
        pyarrow.Table
            Table inventory from information_schema.tables.
        """
        query = (
            "SELECT table_catalog, table_schema, table_name, table_type "
            "FROM information_schema.tables"
        )
        return self.ctx.sql(query).to_arrow_table()

    def table_names_snapshot(self) -> set[str]:
        """Return registered table names from information_schema.

        Returns
        -------
        set[str]
            Set of table names registered in the session.
        """
        names: set[str] = set()
        for row in self.tables_snapshot():
            value = row.get("table_name")
            if value is not None:
                names.add(str(value))
        return names

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
            "SELECT table_catalog, table_schema, table_name, column_name, data_type, "
            "is_nullable, column_default "
            "FROM information_schema.columns"
        )
        return _rows_for_query(self.ctx, query)

    def schema_map(self) -> dict[str, dict[str, dict[str, dict[str, str]]]]:
        """Return a SQLGlot schema mapping derived from information_schema.

        Returns
        -------
        dict[str, dict[str, dict[str, dict[str, str]]]]
            Mapping of catalog -> schema -> table -> column/type mappings.
        """
        rows = self.columns_snapshot()
        mapping: dict[str, dict[str, dict[str, dict[str, str]]]] = {}
        for row in rows:
            catalog = row.get("table_catalog")
            schema = row.get("table_schema")
            table = row.get("table_name")
            column = row.get("column_name")
            dtype = row.get("data_type")
            if not isinstance(table, str) or not isinstance(column, str):
                continue
            if not table or not column:
                continue
            catalog_name = str(catalog) if catalog is not None else "datafusion"
            schema_name = str(schema) if schema is not None else "public"
            mapping.setdefault(catalog_name, {}).setdefault(schema_name, {}).setdefault(table, {})[
                column
            ] = str(dtype) if dtype is not None else "unknown"
        return mapping

    def schema_map_fingerprint(self) -> str:
        """Return a stable fingerprint for the information_schema schema map.

        Returns
        -------
        str
            SHA-256 fingerprint for the schema map payload.
        """
        return schema_map_fingerprint_from_mapping(self.schema_map())

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

    def function_catalog_snapshot(
        self, *, include_parameters: bool = False
    ) -> list[dict[str, object]]:
        """Return a stable snapshot of DataFusion function metadata.

        Returns
        -------
        list[dict[str, object]]
            Sorted function catalog entries derived from information_schema.
        """
        entries: list[dict[str, object]] = []
        for row in self.routines_snapshot():
            payload = dict(row)
            if "routine_name" in payload and "function_name" not in payload:
                payload["function_name"] = payload["routine_name"]
            if "routine_type" in payload and "function_type" not in payload:
                payload["function_type"] = payload["routine_type"]
            payload.setdefault("source", "information_schema")
            entries.append(payload)
        if include_parameters:
            for row in self.parameters_snapshot():
                payload = dict(row)
                if "routine_name" in payload and "function_name" not in payload:
                    payload["function_name"] = payload["routine_name"]
                payload.setdefault("source", "information_schema")
                entries.append(payload)
        return sorted(entries, key=_function_catalog_sort_key)

    def function_names(self) -> set[str]:
        """Return function names from information_schema.routines.

        Returns
        -------
        set[str]
            Function name set from information_schema.
        """
        names: set[str] = set()
        for row in self.routines_snapshot():
            routine_type = row.get("routine_type")
            if routine_type is not None and str(routine_type) != "FUNCTION":
                continue
            name = row.get("routine_name")
            if isinstance(name, str):
                names.add(name)
        return names

    def settings_snapshot(self) -> list[dict[str, object]]:
        """Return session settings from information_schema.df_settings.

        Returns
        -------
        list[dict[str, object]]
            Session settings rows with name/value pairs.
        """
        query = "SELECT name, value FROM information_schema.df_settings"
        return _rows_for_query(self.ctx, query)

    def settings_snapshot_table(self) -> pa.Table:
        """Return session settings as a pyarrow.Table.

        Returns
        -------
        pyarrow.Table
            Table of settings from information_schema.df_settings.
        """
        query = "SELECT name, value FROM information_schema.df_settings"
        return self.ctx.sql(query).to_arrow_table()

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
        return defaults

    def table_column_names(self, table_name: str) -> set[str]:
        """Return column names for a table from information_schema.

        Returns
        -------
        set[str]
            Column name set for the table.
        """
        names: set[str] = set()
        for row in self.table_columns(table_name):
            name = row.get("column_name")
            if name is not None:
                names.add(str(name))
        return names

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


def schema_map_fingerprint_from_mapping(mapping: Mapping[object, object]) -> str:
    """Return a stable fingerprint for a schema mapping.

    Returns
    -------
    str
        SHA-256 fingerprint for the schema mapping payload.
    """
    normalized = _normalize_schema_map(mapping)
    entries: list[dict[str, object]] = []
    for catalog_name, schemas in sorted(normalized.items(), key=lambda item: item[0]):
        for schema_name, tables in sorted(schemas.items(), key=lambda item: item[0]):
            for table_name, columns in sorted(tables.items(), key=lambda item: item[0]):
                column_entries = [
                    {"name": name, "dtype": dtype}
                    for name, dtype in sorted(columns.items(), key=lambda item: item[0])
                ]
                entries.append(
                    {
                        "table": f"{catalog_name}.{schema_name}.{table_name}",
                        "columns": column_entries,
                    }
                )
    payload = {"version": SCHEMA_MAP_HASH_VERSION, "entries": entries}
    return payload_hash(payload, _SCHEMA_MAP_HASH_SCHEMA)


def _normalize_schema_map(
    mapping: Mapping[object, object],
) -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    normalized: dict[str, dict[str, dict[str, dict[str, str]]]] = {}

    def _is_column_map(value: Mapping[object, object]) -> bool:
        return all(not isinstance(item, Mapping) for item in value.values())

    def _store_table(
        *,
        catalog: str,
        schema: str,
        table: str,
        columns: Mapping[object, object],
    ) -> None:
        column_map = {str(name): str(dtype) for name, dtype in columns.items()}
        normalized.setdefault(catalog, {}).setdefault(schema, {})[table] = column_map

    for key, value in mapping.items():
        if not isinstance(value, Mapping):
            continue
        if _is_column_map(value):
            _store_table(
                catalog="datafusion",
                schema="public",
                table=str(key),
                columns=value,
            )
            continue
        for table_key, table_value in value.items():
            if not isinstance(table_value, Mapping):
                continue
            if _is_column_map(table_value):
                _store_table(
                    catalog="datafusion",
                    schema=str(key),
                    table=str(table_key),
                    columns=table_value,
                )
                continue
            for column_key, column_value in table_value.items():
                if not isinstance(column_value, Mapping):
                    continue
                if not _is_column_map(column_value):
                    continue
                _store_table(
                    catalog=str(key),
                    schema=str(table_key),
                    table=str(column_key),
                    columns=column_value,
                )
    return normalized


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


def _function_catalog_sort_key(row: Mapping[str, object]) -> tuple[str, str]:
    name = row.get("function_name")
    func_name = str(name) if name is not None else ""
    func_type = row.get("function_type")
    return func_name, str(func_type) if func_type is not None else ""


__all__ = [
    "SchemaIntrospector",
    "find_struct_field_keys",
    "record_table_definition_override",
    "table_definition_override",
]
