"""Schema introspection helpers for DataFusion sessions.

This module provides schema reflection utilities that source metadata from
DataFusion's catalog and information_schema views. All schema discovery,
constraint validation, and DDL provenance queries use DataFusion as the
canonical source of truth.

Key introspection surfaces:
- information_schema.columns: Column metadata and defaults
- information_schema.tables: Table inventory
- information_schema.table_constraints: Constraint metadata
- information_schema.key_column_usage: Key column definitions
- information_schema.routines: Function/UDF catalog
- information_schema.parameters: Function parameter metadata
- information_schema.df_settings: Session configuration
"""

from __future__ import annotations

import gc
import warnings
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.arrow.interop import coerce_arrow_schema
from datafusion_engine.schema.introspection_cache import SchemaMapping
from datafusion_engine.schema.introspection_common import read_only_sql_options
from datafusion_engine.schema.introspection_routines import _introspection_cache_for_ctx
from datafusion_engine.tables.metadata import table_provider_metadata
from serde_msgspec import to_builtins
from utils.hashing import CacheKeyBuilder

if TYPE_CHECKING:
    from datafusion_engine.catalog.introspection import IntrospectionCache, IntrospectionSnapshot
    from datafusion_engine.schema.contracts import SchemaContract


def schema_map_fingerprint_from_mapping(mapping: SchemaMapping) -> str:
    """Return a stable fingerprint for a schema mapping.

    Parameters
    ----------
    mapping : SchemaMapping
        Nested schema mapping structure.

    Returns:
    -------
    str
        SHA-256 fingerprint for the schema map payload.
    """
    from datafusion_engine.schema.introspection_cache import (
        schema_map_fingerprint_from_mapping as _schema_map_fingerprint_from_mapping,
    )

    return _schema_map_fingerprint_from_mapping(mapping)


def schema_map_fingerprint(introspector: SchemaIntrospector) -> str:
    """Return a stable fingerprint for a schema introspector mapping.

    Parameters
    ----------
    introspector : SchemaIntrospector
        Schema introspector providing the schema mapping.

    Returns:
    -------
    str
        SHA-256 fingerprint for the schema mapping payload.
    """
    from datafusion_engine.schema.introspection_cache import (
        schema_map_fingerprint as _schema_map_fingerprint,
    )

    return _schema_map_fingerprint(introspector)


def schema_contract_from_table(
    ctx: SessionContext,
    *,
    table_name: str,
) -> SchemaContract:
    """Return a SchemaContract derived from a DataFusion table schema.

    Returns:
    -------
    SchemaContract
        Schema contract derived from the catalog schema.
    """
    from datafusion_engine.schema.contracts import SchemaContract

    schema = schema_from_table(ctx, table_name)
    return SchemaContract.from_arrow_schema(table_name, schema)


if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


def _stable_cache_key(prefix: str, payload: Mapping[str, object]) -> str:
    builder = CacheKeyBuilder(prefix=prefix)
    builder.add("payload", to_builtins(payload))
    return builder.build()


def _normalized_rows(table: pa.Table) -> list[dict[str, object]]:
    return [{str(key).lower(): value for key, value in row.items()} for row in table.to_pylist()]


def _sortable_text(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _sortable_int(value: object) -> int:
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


def _row_sort_key(row: Mapping[str, object], keys: Sequence[str]) -> tuple[str, ...]:
    return tuple(_sortable_text(row.get(key)) for key in keys)


def table_names_snapshot(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> set[str]:
    """Return registered table names from information_schema.

    Returns:
    -------
    set[str]
        Set of table names registered in the session.
    """
    try:
        snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
        if "table_name" in snapshot.tables.column_names:
            return {
                str(name) for name in snapshot.tables["table_name"].to_pylist() if name is not None
            }
    except (ValueError, RuntimeError, TypeError, KeyError):
        pass
    tables = getattr(ctx, "tables", None)
    if callable(tables):
        try:
            names = tables()
            if isinstance(names, Iterable) and not isinstance(names, (str, bytes, bytearray)):
                return {str(name) for name in names if name is not None}
        except (RuntimeError, TypeError, ValueError):
            return set()
    return set()


def settings_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return session settings as a pyarrow.Table.

    Returns:
    -------
    pyarrow.Table
        Table of settings from information_schema.df_settings.
    """
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return snapshot.settings


def tables_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return table inventory rows as a pyarrow.Table.

    Returns:
    -------
    pyarrow.Table
        Table inventory from information_schema.tables.
    """
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return snapshot.tables


def routines_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return information_schema.routines as a pyarrow.Table.

    Returns:
    -------
    pyarrow.Table
        Routine inventory from information_schema.routines.
    """
    from datafusion_engine.schema.introspection_routines import (
        routines_snapshot_table as _routines_snapshot_table,
    )

    return _routines_snapshot_table(ctx, sql_options=sql_options)


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
                _introspection_cache_for_ctx(self.ctx, sql_options=self.sql_options),
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

    def invalidate_cache(self, *, tag: str | None = None) -> int:
        """Evict cached schema rows for this introspector.

        Returns:
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

        Args:
            sql: Description.

        Raises:
            TypeError: If the operation cannot be completed.
            ValueError: If the operation cannot be completed.
        """
        cache = self.cache
        payload = {"sql": sql}
        key = self._cache_key("describe_query", payload=payload)
        if cache is not None:
            cached = cache.get(key, default=None, retry=True)
            if isinstance(cached, list):
                return cached
        try:
            df = self.ctx.sql_with_options(sql, self.sql_options or read_only_sql_options())
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = "Schema introspection SQL parse failed."
            raise ValueError(msg) from exc
        schema = coerce_arrow_schema(df.schema())
        if schema is None:
            msg = "Schema introspection failed to resolve a PyArrow schema."
            raise TypeError(msg)
        rows = [
            {
                "column_name": field.name,
                "data_type": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ]
        if cache is not None:
            cache.set(key, rows, expire=self.cache_ttl, tag=self.cache_prefix, retry=True)
        return rows

    def table_columns(self, table_name: str) -> list[dict[str, object]]:
        """Return column metadata from information_schema for a table.

        Returns:
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

        Returns:
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

        Returns:
        -------
        list[dict[str, object]]
            Table inventory rows including catalog/schema/type.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        rows = [dict(row) for row in snapshot.tables.to_pylist()]
        return sorted(
            rows,
            key=lambda row: _row_sort_key(
                row,
                ("table_catalog", "table_schema", "table_name", "table_type"),
            ),
        )

    def schemata_snapshot(self) -> list[dict[str, object]]:
        """Return schema inventory rows from information_schema.

        Returns:
        -------
        list[dict[str, object]]
            Schema inventory rows including catalog and schema names.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        rows = [dict(row) for row in snapshot.schemata.to_pylist()]
        return sorted(
            rows,
            key=lambda row: _row_sort_key(row, ("catalog_name", "schema_name")),
        )

    def columns_snapshot(self) -> list[dict[str, object]]:
        """Return all column metadata rows from information_schema.

        Returns:
        -------
        list[dict[str, object]]
            Column metadata rows for all tables.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        rows = [dict(row) for row in snapshot.columns.to_pylist()]
        return sorted(
            rows,
            key=lambda row: (
                _sortable_text(row.get("table_catalog")),
                _sortable_text(row.get("table_schema")),
                _sortable_text(row.get("table_name")),
                _sortable_int(row.get("ordinal_position")),
                _sortable_text(row.get("column_name")),
            ),
        )

    def table_schema(self, table_name: str) -> pa.Schema:
        """Return Arrow schema from DataFusion catalog for a table.

        This method queries DataFusion's catalog via ctx.table(name).schema() to
        resolve the Arrow schema. All schema validation and contract checks should
        use this method or schema_from_table() to ensure DataFusion is the source
        of truth for schema metadata.

        Parameters
        ----------
        table_name : str
            Table name registered in the catalog.

        Returns:
        -------
        pyarrow.Schema
            Arrow schema resolved from DataFusion catalog.
        """
        return schema_from_table(self.ctx, table_name)

    def routines_snapshot(self) -> list[dict[str, object]]:
        """Return routine inventory rows from information_schema.

        Returns:
        -------
        list[dict[str, object]]
            Routine inventory rows including name and type.
        """
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ResourceWarning)
                table = routines_snapshot_table(self.ctx, sql_options=self.sql_options)
                rows = [dict(row) for row in table.to_pylist()]
                gc.collect()
        except (RuntimeError, TypeError, ValueError, Warning):
            return []
        return sorted(
            rows,
            key=lambda row: _row_sort_key(
                row,
                ("routine_catalog", "routine_schema", "routine_name", "specific_name"),
            ),
        )

    def parameters_snapshot(self) -> list[dict[str, object]]:
        """Return routine parameter rows from information_schema.

        Returns:
        -------
        list[dict[str, object]]
            Parameter metadata rows including names and data types.
        """
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ResourceWarning)
                table = parameters_snapshot_table(self.ctx, sql_options=self.sql_options)
                if table is None:
                    return []
                rows = [dict(row) for row in table.to_pylist()]
                gc.collect()
        except (RuntimeError, TypeError, ValueError, Warning):
            return []
        return sorted(
            rows,
            key=lambda row: (
                _sortable_text(row.get("specific_catalog")),
                _sortable_text(row.get("specific_schema")),
                _sortable_text(row.get("specific_name")),
                _sortable_int(row.get("ordinal_position")),
                _sortable_text(row.get("parameter_name")),
            ),
        )

    def function_catalog_snapshot(
        self, *, include_parameters: bool = False
    ) -> list[dict[str, object]]:
        """Return a stable snapshot of DataFusion function metadata.

        Returns:
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

        Returns:
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

        Returns:
        -------
        list[dict[str, object]]
            Session settings rows with name/value pairs.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return []
        rows = [dict(row) for row in snapshot.settings.to_pylist()]
        return sorted(rows, key=lambda row: _row_sort_key(row, ("name", "setting_name", "key")))

    def table_column_defaults(self, table_name: str) -> dict[str, object]:
        """Return column default metadata for a table when available.

        Returns:
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
        metadata = table_provider_metadata(self.ctx, table_name=table_name)
        if metadata is not None and metadata.default_values:
            for name, value in metadata.default_values.items():
                defaults.setdefault(name, value)
        return defaults

    def table_column_names(self, table_name: str) -> set[str]:
        """Return column names for a table from information_schema.

        Returns:
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

        Returns:
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

        Returns:
        -------
        str | None
            CREATE TABLE statement when available.
        """
        metadata = table_provider_metadata(self.ctx, table_name=table_name)
        return metadata.ddl if metadata else None

    def table_constraints(self, table_name: str) -> tuple[str, ...]:
        """Return constraint expressions for a table when available.

        Parameters
        ----------
        table_name
            Table name to inspect.

        Returns:
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

    def schema_map(self) -> dict[str, dict[str, str]]:
        """Build schema mapping from information_schema snapshots.

        Delegates to the underlying IntrospectionSnapshot.schema_map() method
        to provide a mapping of table names to their columns and types.

        Returns:
        -------
        dict[str, dict[str, str]]
            Mapping of table_name -> {column_name: data_type}.
        """
        snapshot = self.snapshot
        if snapshot is None:
            return {}
        return snapshot.schema_map()


__all__ = [
    "SchemaIntrospector",
    "catalogs_snapshot",
    "constraint_rows",
    "find_struct_field_keys",
    "parameters_snapshot_table",
    "routines_snapshot_table",
    "schema_contract_from_table",
    "schema_from_table",
    "table_constraint_rows",
    "tables_snapshot_table",
]

from datafusion_engine.schema.introspection_helpers import (
    _function_catalog_sort_key,
    catalogs_snapshot,
    constraint_rows,
    find_struct_field_keys,
    parameters_snapshot_table,
    schema_from_table,
    table_constraint_rows,
)
