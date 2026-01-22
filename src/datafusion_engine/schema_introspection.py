"""Schema introspection helpers for DataFusion sessions."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.sql_options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from datafusion_engine.table_provider_metadata import table_provider_metadata
from registry_common.arrow_payloads import payload_hash
from sqlglot_tools.optimizer import SchemaMapping, SchemaMappingNode

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


def _read_only_sql_options() -> SQLOptions:
    return sql_options_for_profile(None)


def _statement_sql_options() -> SQLOptions:
    return statement_sql_options_for_profile(None)


def _table_for_query(
    ctx: SessionContext,
    query: str,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    options = sql_options or _read_only_sql_options()
    return ctx.sql_with_options(query, options).to_arrow_table()


def _rows_for_query(
    ctx: SessionContext,
    query: str,
    *,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return a list of row mappings for a SQL query.

    Returns
    -------
    list[dict[str, object]]
        Rows represented as dictionaries keyed by column name.
    """
    table = _table_for_query(ctx, query, sql_options=sql_options)
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
    return _table_for_query(ctx, query, sql_options=sql_options)


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
    return _table_for_query(ctx, query, sql_options=sql_options)


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
    query = (
        "SELECT routine_catalog, routine_schema, routine_name, routine_type "
        "FROM information_schema.routines"
    )
    return _table_for_query(ctx, query, sql_options=sql_options)


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


def _arrow_type_to_sql_type(arrow_type: pa.DataType) -> str:
    """Convert PyArrow type to DataFusion SQL type string.

    Parameters
    ----------
    arrow_type : pa.DataType
        PyArrow data type to convert.

    Returns
    -------
    str
        SQL type string for CREATE EXTERNAL TABLE DDL.
    """
    scalar_type = _arrow_scalar_sql_type(arrow_type)
    if scalar_type is not None:
        return scalar_type
    if pa.types.is_decimal(arrow_type):
        decimal_type = arrow_type
        return f"DECIMAL({decimal_type.precision}, {decimal_type.scale})"
    if pa.types.is_list(arrow_type):
        value_type = _arrow_type_to_sql_type(arrow_type.value_type)
        return f"{value_type}[]"
    if pa.types.is_struct(arrow_type):
        field_defs = ", ".join(
            f"{struct_field.name} {_arrow_type_to_sql_type(struct_field.type)}"
            for struct_field in arrow_type
        )
        return f"STRUCT({field_defs})"
    if pa.types.is_map(arrow_type):
        key_type = _arrow_type_to_sql_type(arrow_type.key_type)
        item_type = _arrow_type_to_sql_type(arrow_type.item_type)
        return f"MAP({key_type}, {item_type})"
    return "VARCHAR"


def _arrow_scalar_sql_type(arrow_type: pa.DataType) -> str | None:
    checks: tuple[tuple[Callable[[pa.DataType], bool], str], ...] = (
        (pa.types.is_boolean, "BOOLEAN"),
        (pa.types.is_int8, "TINYINT"),
        (pa.types.is_int16, "SMALLINT"),
        (pa.types.is_int32, "INT"),
        (pa.types.is_int64, "BIGINT"),
        (pa.types.is_uint8, "TINYINT UNSIGNED"),
        (pa.types.is_uint16, "SMALLINT UNSIGNED"),
        (pa.types.is_uint32, "INT UNSIGNED"),
        (pa.types.is_uint64, "BIGINT UNSIGNED"),
        (pa.types.is_float32, "FLOAT"),
        (pa.types.is_float64, "DOUBLE"),
        (pa.types.is_string, "VARCHAR"),
        (pa.types.is_large_string, "VARCHAR"),
        (pa.types.is_binary, "BYTEA"),
        (pa.types.is_large_binary, "BYTEA"),
        (pa.types.is_date32, "DATE"),
        (pa.types.is_date64, "DATE"),
        (pa.types.is_timestamp, "TIMESTAMP"),
        (pa.types.is_duration, "INTERVAL"),
    )
    for check, sql_type in checks:
        if check(arrow_type):
            return sql_type
    return None


@dataclass(frozen=True)
class ExternalTableDDLBuilder:
    """Builder for CREATE EXTERNAL TABLE DDL statements.

    Generates DataFusion-compatible DDL for external tables from PyArrow
    schemas and storage locations.
    """

    table_name: str
    schema: pa.Schema
    location: str
    file_format: str = "PARQUET"
    partition_columns: Sequence[str] = ()
    options: Mapping[str, str] = field(default_factory=dict)

    def build_ddl(self) -> str:
        """Build CREATE EXTERNAL TABLE DDL statement.

        Returns
        -------
        str
            DDL statement for registering the external table.
        """
        column_defs = []
        for schema_field in self.schema:
            if schema_field.name in self.partition_columns:
                continue
            sql_type = _arrow_type_to_sql_type(schema_field.type)
            nullable = "" if schema_field.nullable else " NOT NULL"
            column_defs.append(f"  {schema_field.name} {sql_type}{nullable}")

        columns_clause = ",\n".join(column_defs)

        stored_as = self.file_format.upper()

        options_clause = ""
        if self.options:
            option_items = [f"{k} {_sql_literal(v)}" for k, v in self.options.items()]
            options_clause = f"\nOPTIONS ({', '.join(option_items)})"

        partitioned_clause = ""
        if self.partition_columns:
            partition_cols = ", ".join(self.partition_columns)
            partitioned_clause = f"\nPARTITIONED BY ({partition_cols})"

        return (
            f"CREATE EXTERNAL TABLE {self.table_name} (\n"
            f"{columns_clause}\n"
            f")\n"
            f"STORED AS {stored_as}"
            f"{partitioned_clause}"
            f"{options_clause}\n"
            f"LOCATION {_sql_literal(self.location)}"
        )

    def with_options(self, options: Mapping[str, str]) -> ExternalTableDDLBuilder:
        """Return a builder with updated options.

        Parameters
        ----------
        options : Mapping[str, str]
            Storage options to include in the DDL.

        Returns
        -------
        ExternalTableDDLBuilder
            New builder instance with updated options.
        """
        return ExternalTableDDLBuilder(
            table_name=self.table_name,
            schema=self.schema,
            location=self.location,
            file_format=self.file_format,
            partition_columns=self.partition_columns,
            options=options,
        )

    def with_partition_columns(self, partition_columns: Sequence[str]) -> ExternalTableDDLBuilder:
        """Return a builder with updated partition columns.

        Parameters
        ----------
        partition_columns : Sequence[str]
            Partition column names.

        Returns
        -------
        ExternalTableDDLBuilder
            New builder instance with updated partition columns.
        """
        return ExternalTableDDLBuilder(
            table_name=self.table_name,
            schema=self.schema,
            location=self.location,
            file_format=self.file_format,
            partition_columns=partition_columns,
            options=self.options,
        )


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

    def describe_query(self, sql: str) -> list[dict[str, object]]:
        """Return the computed output schema for a SQL query.

        Returns
        -------
        list[dict[str, object]]
            ``DESCRIBE`` rows for the query.
        """
        return _rows_for_query(self.ctx, f"DESCRIBE {sql}", sql_options=self.sql_options)

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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

    def catalogs_snapshot(self) -> list[dict[str, object]]:
        """Return catalog inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Catalog inventory rows.
        """
        query = "SELECT DISTINCT catalog_name FROM information_schema.schemata"
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

    def schemata_snapshot(self) -> list[dict[str, object]]:
        """Return schema inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Schema inventory rows including catalog and schema names.
        """
        query = "SELECT catalog_name, schema_name FROM information_schema.schemata"
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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

    def _schema_map_for_sqlglot(self) -> SchemaMapping:
        """Return a SQLGlot-compatible nested schema mapping.

        Returns a nested mapping structure compatible with SQLGlot's SchemaMapping
        type, which has the form: {catalog: {schema: {table: {column: type}}}}.

        Returns
        -------
        SchemaMapping
            Nested mapping structure for SQLGlot optimizer.
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
            dtype_str = str(dtype) if dtype is not None else "unknown"
            mapping.setdefault(catalog_name, {}).setdefault(schema_name, {}).setdefault(table, {})[
                column
            ] = dtype_str
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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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
            return _table_for_query(self.ctx, query, sql_options=self.sql_options)
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
        return _rows_for_query(self.ctx, query, sql_options=self.sql_options)

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
            rows = _rows_for_query(
                self.ctx,
                f"SHOW CREATE TABLE {table_name}",
                sql_options=sql_options,
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
            rows = _rows_for_query(
                self.ctx,
                "SELECT constraint_name, constraint_type, constraint_definition "
                "FROM information_schema.table_constraints "
                f"WHERE table_name = {_sql_literal(table_name)}",
                sql_options=self.sql_options,
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


def schema_map_fingerprint_from_mapping(mapping: SchemaMapping) -> str:
    """Return a stable fingerprint for a schema mapping.

    Returns
    -------
    str
        SHA-256 fingerprint for the schema mapping payload.
    """
    flat = _flatten_schema_mapping(mapping)
    entries: list[dict[str, object]] = []
    for table_name, columns in sorted(flat.items(), key=lambda item: item[0]):
        column_entries = [
            {"name": name, "dtype": dtype}
            for name, dtype in sorted(columns.items(), key=lambda item: item[0])
        ]
        entries.append({"table": table_name, "columns": column_entries})
    payload = {"version": SCHEMA_MAP_HASH_VERSION, "entries": entries}
    return payload_hash(payload, _SCHEMA_MAP_HASH_SCHEMA)


def _is_leaf_schema_node(node: SchemaMappingNode) -> bool:
    return all(not isinstance(value, Mapping) for value in node.values())


def _flatten_schema_mapping(mapping: SchemaMapping) -> dict[str, dict[str, str]]:
    flat: dict[str, dict[str, str]] = {}

    def _visit(node: SchemaMappingNode, path: list[str]) -> None:
        if _is_leaf_schema_node(node):
            table_key = ".".join(path)
            flat[table_key] = {str(key): str(value) for key, value in node.items()}
            return
        for key, value in node.items():
            if not isinstance(value, Mapping):
                continue
            _visit(value, [*path, str(key)])

    for key, value in mapping.items():
        if not isinstance(value, Mapping):
            continue
        _visit(value, [str(key)])
    return flat


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


__all__ = [
    "ExternalTableDDLBuilder",
    "SchemaIntrospector",
    "find_struct_field_keys",
    "routines_snapshot_table",
    "tables_snapshot_table",
]
