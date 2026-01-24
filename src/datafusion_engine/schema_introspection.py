"""Schema introspection helpers for DataFusion sessions."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions
from diskcache import memoize_stampede

from datafusion_engine.sql_options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from datafusion_engine.table_provider_metadata import table_provider_metadata
from ibis_engine.schema_utils import sqlglot_column_defs
from serde_msgspec import dumps_msgpack, to_builtins
from sqlglot_tools.optimizer import SchemaMapping, schema_map_fingerprint_from_mapping

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


def _read_only_sql_options() -> SQLOptions:
    return sql_options_for_profile(None)


def _statement_sql_options() -> SQLOptions:
    return statement_sql_options_for_profile(None)


def _stable_cache_key(prefix: str, payload: Mapping[str, object]) -> str:
    raw = dumps_msgpack(to_builtins(payload))
    digest = hashlib.sha256(raw).hexdigest()
    return f"{prefix}:{digest}"


def _table_for_query(
    ctx: SessionContext,
    query: str,
    *,
    sql_options: SQLOptions | None = None,
    prepared_name: str | None = None,
) -> pa.Table:
    options = sql_options or _read_only_sql_options()
    if prepared_name is not None:
        try:
            return ctx.sql_with_options(f"EXECUTE {prepared_name}", options).to_arrow_table()
        except (RuntimeError, TypeError, ValueError):
            pass
    return ctx.sql_with_options(query, options).to_arrow_table()


def _rows_for_query(
    ctx: SessionContext,
    query: str,
    *,
    sql_options: SQLOptions | None = None,
    prepared_name: str | None = None,
) -> list[dict[str, object]]:
    """Return a list of row mappings for a SQL query.

    Returns
    -------
    list[dict[str, object]]
        Rows represented as dictionaries keyed by column name.
    """
    table = _table_for_query(
        ctx,
        query,
        sql_options=sql_options,
        prepared_name=prepared_name,
    )
    return [dict(row) for row in table.to_pylist()]


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def table_names_snapshot(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> set[str]:
    """Return registered table names from information_schema.

    Returns
    -------
    set[str]
        Set of table names registered in the session.
    """
    names: set[str] = set()
    for row in _rows_for_query(
        ctx,
        "SELECT table_name FROM information_schema.tables",
        sql_options=sql_options,
        prepared_name="table_names_snapshot",
    ):
        value = row.get("table_name")
        if value is not None:
            names.add(str(value))
    return names


def settings_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return session settings as a pyarrow.Table.

    Returns
    -------
    pyarrow.Table
        Table of settings from information_schema.df_settings.
    """
    query = "SELECT name, value FROM information_schema.df_settings"
    return _table_for_query(
        ctx,
        query,
        sql_options=sql_options,
        prepared_name="df_settings_snapshot",
    )


def tables_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return table inventory rows as a pyarrow.Table.

    Returns
    -------
    pyarrow.Table
        Table inventory from information_schema.tables.
    """
    query = (
        "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables"
    )
    return _table_for_query(
        ctx,
        query,
        sql_options=sql_options,
        prepared_name="tables_snapshot",
    )


def routines_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return information_schema.routines as a pyarrow.Table.

    Returns
    -------
    pyarrow.Table
        Routine inventory from information_schema.routines.
    """
    query = "SELECT * FROM information_schema.routines"
    return _table_for_query(
        ctx,
        query,
        sql_options=sql_options,
        prepared_name="routines_snapshot",
    )


def table_constraint_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return constraint metadata rows for a table when available.

    Returns
    -------
    list[dict[str, object]]
        Rows including constraint type and column names where available.
    """
    query = (
        "SELECT "
        "tc.table_catalog, "
        "tc.table_schema, "
        "tc.table_name, "
        "tc.constraint_name, "
        "tc.constraint_type, "
        "kcu.column_name, "
        "kcu.ordinal_position "
        "FROM information_schema.table_constraints tc "
        "LEFT JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_name = kcu.constraint_name "
        "AND tc.table_catalog = kcu.table_catalog "
        "AND tc.table_schema = kcu.table_schema "
        "AND tc.table_name = kcu.table_name "
        f"WHERE tc.table_name = {_sql_literal(table_name)} "
        "ORDER BY tc.constraint_name, kcu.ordinal_position"
    )
    return _rows_for_query(ctx, query, sql_options=sql_options)


def constraint_rows(
    ctx: SessionContext,
    *,
    catalog: str | None = None,
    schema: str | None = None,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return constraint metadata rows across tables.

    Returns
    -------
    list[dict[str, object]]
        Rows including constraint type and column names where available.
    """
    filters: list[str] = []
    if catalog is not None:
        filters.append(f"tc.table_catalog = {_sql_literal(catalog)}")
    if schema is not None:
        filters.append(f"tc.table_schema = {_sql_literal(schema)}")
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = (
        "SELECT "
        "tc.table_catalog, "
        "tc.table_schema, "
        "tc.table_name, "
        "tc.constraint_name, "
        "tc.constraint_type, "
        "kcu.column_name, "
        "kcu.ordinal_position "
        "FROM information_schema.table_constraints tc "
        "LEFT JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_name = kcu.constraint_name "
        "AND tc.table_catalog = kcu.table_catalog "
        "AND tc.table_schema = kcu.table_schema "
        "AND tc.table_name = kcu.table_name "
        f"{where_clause} "
        "ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position"
    )
    return _rows_for_query(ctx, query, sql_options=sql_options)


def _ddl_column_defs(schema: pa.Schema, *, partition_columns: set[str]) -> list[str]:
    sqlglot_defs = sqlglot_column_defs(schema, dialect="datafusion")
    if len(sqlglot_defs) != len(schema):
        msg = "SQLGlot column definitions must match schema column count."
        raise ValueError(msg)
    column_defs: list[str] = []
    for schema_field, column_def in zip(schema, sqlglot_defs, strict=True):
        if schema_field.name in partition_columns:
            continue
        column_sql = column_def.sql(dialect="datafusion")
        if not schema_field.nullable and "NOT NULL" not in column_sql.upper():
            column_sql = f"{column_sql} NOT NULL"
        column_defs.append(f"  {column_sql}")
    return column_defs


def _table_name_from_ddl(ddl: str) -> str:
    """Extract table name from CREATE EXTERNAL TABLE DDL.

    Parameters
    ----------
    ddl : str
        CREATE EXTERNAL TABLE DDL statement.

    Returns
    -------
    str
        Extracted table name.

    Raises
    ------
    ValueError
        If the table name cannot be extracted from the DDL.
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


@dataclass(frozen=True)
class SchemaIntrospector:
    """Expose schema reflection across tables, queries, and settings."""

    ctx: SessionContext
    sql_options: SQLOptions | None = None
    cache: Cache | FanoutCache | None = None
    cache_prefix: str | None = None
    cache_ttl: float | None = None

    def _cache_key(self, kind: str, *, payload: Mapping[str, object] | None = None) -> str:
        key_payload = {
            "prefix": self.cache_prefix,
            "kind": kind,
            "payload": payload or {},
        }
        return _stable_cache_key("schema", key_payload)

    def _cached_rows(
        self,
        kind: str,
        *,
        query: str,
        prepared_name: str | None = None,
        sql_options: SQLOptions | None = None,
        payload: Mapping[str, object] | None = None,
    ) -> list[dict[str, object]]:
        cache = self.cache
        if cache is None:
            return _rows_for_query(
                self.ctx,
                query,
                sql_options=sql_options or self.sql_options,
                prepared_name=prepared_name,
            )
        key = self._cache_key(kind, payload=payload)
        cached = cache.get(key, default=None, retry=True)
        if isinstance(cached, list):
            return cached
        rows = _rows_for_query(
            self.ctx,
            query,
            sql_options=sql_options or self.sql_options,
            prepared_name=prepared_name,
        )
        cache.set(key, rows, expire=self.cache_ttl, tag=self.cache_prefix, retry=True)
        return rows

    def invalidate_cache(self, *, tag: str | None = None) -> int:
        """Evict cached schema rows for this introspector.

        Returns
        -------
        int
            Count of evicted cache entries.
        """
        cache = self.cache
        if cache is None:
            return 0
        cache_tag = tag or self.cache_prefix
        if not cache_tag:
            return 0
        return int(cache.evict(cache_tag, retry=True))

    def describe_query(self, sql: str) -> list[dict[str, object]]:
        """Return the computed output schema for a SQL query.

        Returns
        -------
        list[dict[str, object]]
            ``DESCRIBE`` rows for the query.
        """
        query = f"DESCRIBE {sql}"
        return self._cached_rows(
            "describe_query",
            query=query,
            payload={"sql": sql},
        )

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
            f"WHERE table_name = {_sql_literal(table_name)}"
        )
        return self._cached_rows(
            "table_columns",
            query=query,
            payload={"table_name": table_name},
        )

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
            f"WHERE table_name = {_sql_literal(table_name)} "
            "ORDER BY ordinal_position"
        )
        return self._cached_rows(
            "table_columns_with_ordinal",
            query=query,
            payload={"table_name": table_name},
        )

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
        return self._cached_rows("tables_snapshot", query=query)

    def _catalogs_snapshot(self) -> list[dict[str, object]]:
        """Return catalog inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Catalog inventory rows.
        """
        query = "SELECT DISTINCT catalog_name FROM information_schema.schemata"
        return self._cached_rows("catalogs_snapshot", query=query)


    def schemata_snapshot(self) -> list[dict[str, object]]:
        """Return schema inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Schema inventory rows including catalog and schema names.
        """
        query = "SELECT catalog_name, schema_name FROM information_schema.schemata"
        return self._cached_rows("schemata_snapshot", query=query)

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
        return self._cached_rows("columns_snapshot", query=query)

    def schema_map(self) -> dict[str, dict[str, str]]:
        """Return a SQLGlot schema mapping derived from information_schema.

        Returns
        -------
        dict[str, dict[str, str]]
            Mapping of fully qualified table name to column/type mappings.
        """
        rows = self.columns_snapshot()
        mapping: dict[str, dict[str, str]] = {}
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
            table_key = f"{catalog_name}.{schema_name}.{table}"
            mapping.setdefault(table_key, {})[column] = (
                str(dtype) if dtype is not None else "unknown"
            )
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
        query = "SELECT * FROM information_schema.routines"
        return self._cached_rows(
            "routines_snapshot",
            query=query,
            prepared_name="routines_snapshot",
        )

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
        return self._cached_rows(
            "parameters_snapshot",
            query=query,
            prepared_name="parameters_snapshot",
        )

    def parameters_snapshot_table(self) -> pa.Table | None:
        """Return information_schema.parameters as Arrow table if available.

        Returns
        -------
        pyarrow.Table | None
            Parameter inventory from information_schema.parameters, or None if unavailable.
        """
        query = (
            "SELECT specific_name, routine_name, parameter_name, parameter_mode, "
            "data_type, ordinal_position "
            "FROM information_schema.parameters"
        )
        try:
            return _table_for_query(
                self.ctx,
                query,
                sql_options=self.sql_options,
                prepared_name="parameters_snapshot",
            )
        except (RuntimeError, TypeError, ValueError, KeyError):
            return None

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
        return self._cached_rows("settings_snapshot", query=query)

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
            sql_options = self.sql_options or _statement_sql_options()
            rows = self._cached_rows(
                "table_definition",
                query=f"SHOW CREATE TABLE {table_name}",
                sql_options=sql_options,
                payload={"table_name": table_name},
            )
        except (RuntimeError, TypeError, ValueError):
            metadata = table_provider_metadata(id(self.ctx), table_name=table_name)
            return metadata.ddl if metadata else None
        if not rows:
            metadata = table_provider_metadata(id(self.ctx), table_name=table_name)
            return metadata.ddl if metadata else None
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
            rows = self._cached_rows(
                "table_constraints",
                query=(
                    "SELECT constraint_name, constraint_type, constraint_definition "
                    "FROM information_schema.table_constraints "
                    f"WHERE table_name = {_sql_literal(table_name)}"
                ),
                payload={"table_name": table_name},
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


def schema_map_for_sqlglot(introspector: SchemaIntrospector) -> SchemaMapping:
    """Return a SQLGlot-compatible nested schema mapping.

    Returns a nested mapping structure compatible with SQLGlot's SchemaMapping
    type, which has the form: {catalog: {schema: {table: {column: type}}}}.

    Returns
    -------
    SchemaMapping
        Nested schema mapping for SQLGlot qualification and validation.
    """
    rows = introspector.columns_snapshot()
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
        dtype_str = str(dtype) if dtype is not None else "unknown"
        mapping.setdefault(catalog_name, {}).setdefault(schema_name, {}).setdefault(table, {})[
            column
        ] = dtype_str
    return mapping


def schema_map_snapshot(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
    cache: Cache | FanoutCache | None = None,
    cache_key: str | None = None,
    cache_ttl: float | None = None,
    cache_tag: str | None = None,
) -> tuple[SchemaMapping | None, str | None]:
    """Return a SQLGlot schema mapping and fingerprint snapshot.

    Returns
    -------
    tuple[SchemaMapping | None, str | None]
        Schema mapping with its fingerprint, or ``(None, None)`` on failure.
    """
    def _compute() -> tuple[SchemaMapping | None, str | None]:
        try:
            introspector = SchemaIntrospector(ctx, sql_options=sql_options)
            mapping = schema_map_for_sqlglot(introspector)
            return mapping, schema_map_fingerprint_from_mapping(mapping)
        except (RuntimeError, TypeError, ValueError):
            return None, None

    if cache is None or cache_key is None:
        return _compute()

    @memoize_stampede(
        cache,
        expire=cache_ttl,
        tag=cache_tag,
        name="schema_map_snapshot",
    )
    def _cached(key: str) -> tuple[SchemaMapping | None, str | None]:
        _ = key
        return _compute()

    return _cached(cache_key)


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

    Returns
    -------
    list[dict[str, object]]
        Catalog inventory rows.
    """
    return introspector._catalogs_snapshot()


__all__ = [
    "SchemaIntrospector",
    "catalogs_snapshot",
    "find_struct_field_keys",
    "routines_snapshot_table",
    "tables_snapshot_table",
]
