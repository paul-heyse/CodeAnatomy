"""Schema introspection helpers for DataFusion sessions."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions
from diskcache import memoize_stampede

from datafusion_engine.introspection import (
    IntrospectionCache,
    IntrospectionSnapshot,
    introspection_cache_for_ctx,
)
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
    from sqlglot.errors import ParseError

    from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from sqlglot_tools.optimizer import parse_sql_strict, register_datafusion_dialect

    def _sql_ingest(_payload: Mapping[str, object]) -> None:
        return None

    if prepared_name is not None:
        try:
            allow_statements = True
            statement_options = DataFusionCompileOptions(
                sql_options=options.with_allow_statements(allow_statements),
                sql_policy=DataFusionSqlPolicy(allow_statements=True),
                sql_ingest_hook=_sql_ingest,
            )
            facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
            try:
                register_datafusion_dialect()
                statement_expr = parse_sql_strict(
                    f"EXECUTE {prepared_name}",
                    dialect=statement_options.dialect,
                )
            except (ParseError, TypeError, ValueError) as exc:
                msg = "Prepared statement SQL parse failed."
                raise ValueError(msg) from exc
            plan = facade.compile(statement_expr, options=statement_options)
            result = facade.execute(plan)
        except (RuntimeError, TypeError, ValueError):
            pass
        else:
            if result.dataframe is not None:
                return result.dataframe.to_arrow_table()
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    query_options = DataFusionCompileOptions(
        sql_options=options,
        sql_policy=DataFusionSqlPolicy(),
        sql_ingest_hook=_sql_ingest,
    )
    try:
        register_datafusion_dialect()
        query_expr = parse_sql_strict(query, dialect=query_options.dialect)
    except (ParseError, TypeError, ValueError) as exc:
        msg = "Schema introspection SQL parse failed."
        raise ValueError(msg) from exc
    plan = facade.compile(query_expr, options=query_options)
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "Schema introspection SQL did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe.to_arrow_table()


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


def _normalized_rows(table: pa.Table) -> list[dict[str, object]]:
    return [{str(key).lower(): value for key, value in row.items()} for row in table.to_pylist()]


def _constraint_rows_from_snapshot(
    snapshot: IntrospectionSnapshot,
    *,
    table_name: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
) -> list[dict[str, object]]:
    if snapshot.table_constraints is None or snapshot.key_column_usage is None:
        return []
    constraints = _normalized_rows(snapshot.table_constraints)
    usage_rows = _normalized_rows(snapshot.key_column_usage)
    usage_map: dict[
        tuple[str | None, str | None, str | None, str | None], list[dict[str, object]]
    ] = {}
    for row in usage_rows:
        key = (
            str(row.get("table_catalog")) if row.get("table_catalog") is not None else None,
            str(row.get("table_schema")) if row.get("table_schema") is not None else None,
            str(row.get("table_name")) if row.get("table_name") is not None else None,
            str(row.get("constraint_name")) if row.get("constraint_name") is not None else None,
        )
        usage_map.setdefault(key, []).append(row)
    rows: list[dict[str, object]] = []
    for constraint in constraints:
        table_catalog = (
            str(constraint.get("table_catalog"))
            if constraint.get("table_catalog") is not None
            else None
        )
        table_schema = (
            str(constraint.get("table_schema"))
            if constraint.get("table_schema") is not None
            else None
        )
        table_value = (
            str(constraint.get("table_name")) if constraint.get("table_name") is not None else None
        )
        if table_name is not None and table_value != table_name:
            continue
        if catalog is not None and table_catalog != catalog:
            continue
        if schema is not None and table_schema != schema:
            continue
        constraint_name = (
            str(constraint.get("constraint_name"))
            if constraint.get("constraint_name") is not None
            else None
        )
        key = (table_catalog, table_schema, table_value, constraint_name)
        usage = usage_map.get(key)
        if not usage:
            rows.append(
                {
                    "table_catalog": constraint.get("table_catalog"),
                    "table_schema": constraint.get("table_schema"),
                    "table_name": constraint.get("table_name"),
                    "constraint_name": constraint.get("constraint_name"),
                    "constraint_type": constraint.get("constraint_type"),
                    "column_name": None,
                    "ordinal_position": None,
                }
            )
            continue
        rows.extend(
            [
                {
                    "table_catalog": constraint.get("table_catalog"),
                    "table_schema": constraint.get("table_schema"),
                    "table_name": constraint.get("table_name"),
                    "constraint_name": constraint.get("constraint_name"),
                    "constraint_type": constraint.get("constraint_type"),
                    "column_name": entry.get("column_name"),
                    "ordinal_position": entry.get("ordinal_position"),
                }
                for entry in usage
            ]
        )
    return rows


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
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    if "table_name" not in snapshot.tables.column_names:
        return set()
    return {str(name) for name in snapshot.tables["table_name"].to_pylist() if name is not None}


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
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return snapshot.settings


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
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return snapshot.tables


def _empty_routines_table() -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=[
            "specific_catalog",
            "specific_schema",
            "specific_name",
            "routine_catalog",
            "routine_schema",
            "routine_name",
            "routine_type",
            "data_type",
        ],
    )


def _empty_parameters_table() -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.int32()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=[
            "specific_catalog",
            "specific_schema",
            "specific_name",
            "routine_catalog",
            "routine_schema",
            "routine_name",
            "ordinal_position",
            "parameter_name",
            "data_type",
            "parameter_mode",
        ],
    )


def _registry_snapshot(ctx: SessionContext) -> Mapping[str, object] | None:
    try:
        from datafusion_engine.udf_runtime import rust_udf_snapshot
    except ImportError:
        return None
    try:
        snapshot = rust_udf_snapshot(ctx)
    except (RuntimeError, TypeError, ValueError):
        return None
    if not isinstance(snapshot, Mapping):
        return None
    return snapshot


def _registry_names(snapshot: Mapping[str, object], key: str) -> list[str]:
    value = snapshot.get(key, [])
    if isinstance(value, str):
        return []
    if isinstance(value, Iterable):
        return [str(name) for name in value if name is not None]
    return []


def _routine_name_from_row(row: Mapping[str, object]) -> str | None:
    for key in ("routine_name", "function_name", "name"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _normalized_aliases(value: object) -> Mapping[str, Sequence[str]]:
    if isinstance(value, Mapping):
        resolved: dict[str, Sequence[str]] = {}
        for key, names in value.items():
            if names is None or isinstance(names, str):
                resolved[str(key)] = ()
            elif isinstance(names, Iterable):
                resolved[str(key)] = tuple(str(name) for name in names if name is not None)
            else:
                resolved[str(key)] = ()
        return resolved
    return {}


def _normalized_parameter_names(value: object) -> Mapping[str, Sequence[str]]:
    if isinstance(value, Mapping):
        resolved: dict[str, Sequence[str]] = {}
        for key, names in value.items():
            if names is None or isinstance(names, str):
                resolved[str(key)] = ()
            elif isinstance(names, Iterable):
                resolved[str(key)] = tuple(str(name) for name in names if name is not None)
            else:
                resolved[str(key)] = ()
        return resolved
    return {}


def _deterministic_flag(volatility: str | None) -> bool | None:
    if volatility is None:
        return None
    lowered = volatility.lower()
    if lowered in {"immutable", "stable"}:
        return True
    if lowered == "volatile":
        return False
    return None


def _routine_row(
    *,
    name: str,
    routine_type: str,
    specific_name: str,
    volatility: str | None,
    schema_names: Sequence[str],
) -> dict[str, object]:
    row: dict[str, object] = {
        "specific_catalog": "datafusion",
        "specific_schema": "public",
        "specific_name": specific_name,
        "routine_catalog": "datafusion",
        "routine_schema": "public",
        "routine_name": name,
        "routine_type": routine_type,
        "data_type": None,
    }
    if "function_name" in schema_names:
        row["function_name"] = name
    if "function_type" in schema_names:
        row["function_type"] = routine_type
    if "volatility" in schema_names:
        row["volatility"] = volatility
    if "is_deterministic" in schema_names:
        row["is_deterministic"] = _deterministic_flag(volatility)
    return row


def _parameter_row(
    *,
    name: str,
    specific_name: str,
    ordinal: int,
    param_name: str,
    schema_names: Sequence[str],
) -> dict[str, object]:
    row: dict[str, object] = {
        "specific_catalog": "datafusion",
        "specific_schema": "public",
        "specific_name": specific_name,
        "routine_catalog": "datafusion",
        "routine_schema": "public",
        "routine_name": name,
        "ordinal_position": ordinal,
        "parameter_name": param_name,
        "data_type": "unknown",
        "parameter_mode": "IN",
    }
    if "function_name" in schema_names:
        row["function_name"] = name
    return row


def _aligned_rows(
    rows: Sequence[Mapping[str, object]],
    schema_names: Sequence[str],
) -> list[dict[str, object]]:
    return [{name: row.get(name) for name in schema_names} for row in rows]


def _merge_registry_routines(ctx: SessionContext, base: pa.Table) -> pa.Table:
    snapshot = _registry_snapshot(ctx)
    if snapshot is None:
        return base
    schema_names = tuple(base.schema.names)
    aliases = _normalized_aliases(snapshot.get("aliases"))
    volatility = snapshot.get("volatility", {})
    volatility_map = (
        {str(key): str(value) for key, value in volatility.items()}
        if isinstance(volatility, Mapping)
        else {}
    )
    routines: list[dict[str, object]] = base.to_pylist()
    known: set[str] = set()
    for row in routines:
        name = _routine_name_from_row(row)
        if name is not None:
            known.add(name.lower())
    registry_kinds = (
        ("scalar", "FUNCTION"),
        ("aggregate", "AGGREGATE"),
        ("window", "WINDOW"),
        ("table", "TABLE"),
    )
    for key, routine_type in registry_kinds:
        for name in _registry_names(snapshot, key):
            lowered = name.lower()
            if lowered in known:
                continue
            routines.append(
                _routine_row(
                    name=name,
                    routine_type=routine_type,
                    specific_name=name,
                    volatility=volatility_map.get(name),
                    schema_names=schema_names,
                )
            )
            known.add(lowered)
            for alias in aliases.get(name, ()):
                alias_lower = alias.lower()
                if alias_lower in known:
                    continue
                routines.append(
                    _routine_row(
                        name=alias,
                        routine_type=routine_type,
                        specific_name=name,
                        volatility=volatility_map.get(name),
                        schema_names=schema_names,
                    )
                )
                known.add(alias_lower)
    if not routines:
        return base
    return pa.Table.from_pylist(_aligned_rows(routines, schema_names), schema=base.schema)


def _merge_registry_parameters(ctx: SessionContext, base: pa.Table | None) -> pa.Table:
    snapshot = _registry_snapshot(ctx)
    base_table = base or _empty_parameters_table()
    if snapshot is None:
        return base_table
    schema_names = tuple(base_table.schema.names)
    parameter_names = _normalized_parameter_names(snapshot.get("parameter_names"))
    routines: list[dict[str, object]] = base_table.to_pylist()
    known: set[str] = set()
    for row in routines:
        name = _routine_name_from_row(row)
        if name is not None:
            known.add(name.lower())
    for name, params in parameter_names.items():
        if name.lower() in known:
            continue
        for ordinal, param_name in enumerate(params, start=1):
            routines.append(
                _parameter_row(
                    name=name,
                    specific_name=name,
                    ordinal=ordinal,
                    param_name=param_name,
                    schema_names=schema_names,
                )
            )
        known.add(name.lower())
    if not routines:
        return base_table
    return pa.Table.from_pylist(_aligned_rows(routines, schema_names), schema=base_table.schema)


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
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    base = snapshot.routines if snapshot.routines is not None else _empty_routines_table()
    return _merge_registry_routines(ctx, base)


def parameters_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table | None:
    """Return information_schema.parameters as a pyarrow.Table when available.

    Returns
    -------
    pyarrow.Table | None
        Parameter inventory from information_schema.parameters, or None if unavailable.
    """
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    base = snapshot.parameters
    merged = _merge_registry_parameters(ctx, base)
    if base is None and merged.num_rows == 0:
        return None
    return merged


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
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return _constraint_rows_from_snapshot(snapshot, table_name=table_name)


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
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return _constraint_rows_from_snapshot(snapshot, catalog=catalog, schema=schema)


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
    introspection_cache: IntrospectionCache | None = None
    snapshot: IntrospectionSnapshot | None = None

    def __post_init__(self) -> None:
        """Attach or derive the shared introspection cache for this session."""
        if self.introspection_cache is None:
            object.__setattr__(
                self,
                "introspection_cache",
                introspection_cache_for_ctx(self.ctx, sql_options=self.sql_options),
            )
        if self.snapshot is None and self.introspection_cache is not None:
            object.__setattr__(self, "snapshot", self.introspection_cache.snapshot)

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
        snapshot = self.snapshot
        if snapshot is None:
            return []
        rows: list[dict[str, object]] = []
        for row in snapshot.columns.to_pylist():
            name = row.get("table_name")
            if name is None or str(name) != table_name:
                continue
            rows.append(
                {
                    "table_catalog": row.get("table_catalog"),
                    "table_schema": row.get("table_schema"),
                    "table_name": row.get("table_name"),
                    "column_name": row.get("column_name"),
                    "data_type": row.get("data_type"),
                    "is_nullable": row.get("is_nullable"),
                    "column_default": row.get("column_default"),
                }
            )
        return rows

    def table_columns_with_ordinal(self, table_name: str) -> list[dict[str, object]]:
        """Return ordered column metadata rows for a table.

        Returns
        -------
        list[dict[str, object]]
            Column metadata rows ordered by ordinal position.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        rows: list[dict[str, object]] = []
        for row in snapshot.columns.to_pylist():
            name = row.get("table_name")
            if name is None or str(name) != table_name:
                continue
            rows.append(
                {
                    "table_catalog": row.get("table_catalog"),
                    "table_schema": row.get("table_schema"),
                    "table_name": row.get("table_name"),
                    "column_name": row.get("column_name"),
                    "data_type": row.get("data_type"),
                    "ordinal_position": row.get("ordinal_position"),
                    "is_nullable": row.get("is_nullable"),
                    "column_default": row.get("column_default"),
                }
            )

        def _ordinal_value(item: Mapping[str, object]) -> int:
            value = item.get("ordinal_position")
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            if isinstance(value, str):
                try:
                    return int(value)
                except ValueError:
                    return 0
            return 0

        return sorted(rows, key=_ordinal_value)

    def tables_snapshot(self) -> list[dict[str, object]]:
        """Return table inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Table inventory rows including catalog/schema/type.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        return [dict(row) for row in snapshot.tables.to_pylist()]

    def schemata_snapshot(self) -> list[dict[str, object]]:
        """Return schema inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Schema inventory rows including catalog and schema names.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        return [dict(row) for row in snapshot.schemata.to_pylist()]

    def columns_snapshot(self) -> list[dict[str, object]]:
        """Return all column metadata rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Column metadata rows for all tables.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        return [dict(row) for row in snapshot.columns.to_pylist()]

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

    def routines_snapshot(self) -> list[dict[str, object]]:
        """Return routine inventory rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Routine inventory rows including name and type.
        """
        table = routines_snapshot_table(self.ctx, sql_options=self.sql_options)
        return [dict(row) for row in table.to_pylist()]

    def parameters_snapshot(self) -> list[dict[str, object]]:
        """Return routine parameter rows from information_schema.

        Returns
        -------
        list[dict[str, object]]
            Parameter metadata rows including names and data types.
        """
        table = parameters_snapshot_table(self.ctx, sql_options=self.sql_options)
        if table is None:
            return []
        return [dict(row) for row in table.to_pylist()]

    def parameters_snapshot_table(self) -> pa.Table | None:
        """Return information_schema.parameters as Arrow table if available.

        Returns
        -------
        pyarrow.Table | None
            Parameter inventory from information_schema.parameters, or None if unavailable.
        """
        return parameters_snapshot_table(self.ctx, sql_options=self.sql_options)

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
        snapshot = self.snapshot
        if snapshot is None:
            return []
        return [dict(row) for row in snapshot.settings.to_pylist()]

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
        snapshot = self.snapshot
        if snapshot is None or snapshot.table_constraints is None:
            return ()
        rows = _normalized_rows(snapshot.table_constraints)
        constraints: list[str] = []
        for row in rows:
            name = row.get("table_name")
            if name is None or str(name) != table_name:
                continue
            definition = row.get("constraint_definition")
            constraint_name = row.get("constraint_name")
            if definition:
                constraints.append(str(definition))
            elif constraint_name:
                constraints.append(str(constraint_name))
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


def schema_map_fingerprint(introspector: SchemaIntrospector) -> str:
    """Return a stable fingerprint for an introspector's schema map.

    Returns
    -------
    str
        SHA-256 fingerprint for the schema map payload.
    """
    return schema_map_fingerprint_from_mapping(introspector.schema_map())


def schema_map_snapshot(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
    cache_options: SchemaMapCacheOptions | None = None,
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

    if cache_options is None:
        return _compute()

    @memoize_stampede(
        cache_options.cache,
        expire=cache_options.ttl,
        tag=cache_options.tag,
        name="schema_map_snapshot",
    )
    def _cached(key: str) -> tuple[SchemaMapping | None, str | None]:
        _ = key
        return _compute()

    return _cached(cache_options.key)


@dataclass(frozen=True)
class SchemaMapCacheOptions:
    """Cache options for schema map snapshots."""

    cache: Cache | FanoutCache
    key: str
    ttl: float | None = None
    tag: str | None = None


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
    seen: set[str] = set()
    rows: list[dict[str, object]] = []
    for row in introspector.schemata_snapshot():
        name = row.get("catalog_name")
        if name is None:
            continue
        catalog = str(name)
        if catalog in seen:
            continue
        seen.add(catalog)
        rows.append({"catalog_name": catalog})
    return rows


__all__ = [
    "SchemaIntrospector",
    "SchemaMapCacheOptions",
    "catalogs_snapshot",
    "find_struct_field_keys",
    "parameters_snapshot_table",
    "routines_snapshot_table",
    "tables_snapshot_table",
]
