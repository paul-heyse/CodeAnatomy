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

import contextlib
import gc
import importlib
import re
import warnings
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.arrow.interop import coerce_arrow_schema
from datafusion_engine.sql.options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from datafusion_engine.tables.metadata import table_provider_metadata
from serde_msgspec import to_builtins
from utils.hashing import CacheKeyBuilder, hash_msgpack_canonical, hash_sha256_hex

SchemaMapping = dict[str, dict[str, dict[str, dict[str, str]]]]

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
    return hash_msgpack_canonical(to_builtins(mapping))


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
    mapping = introspector.schema_map()
    wrapped: SchemaMapping = {"default": {"default": mapping}}
    return schema_map_fingerprint_from_mapping(wrapped)


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


def _read_only_sql_options() -> SQLOptions:
    return sql_options_for_profile(None)


def _statement_sql_options() -> SQLOptions:
    return statement_sql_options_for_profile(None)


def _introspection_cache_for_ctx(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
) -> IntrospectionCache:
    try:
        from datafusion_engine.catalog.introspection import introspection_cache_for_ctx
    except ImportError as exc:  # pragma: no cover - defensive fallback
        msg = "DataFusion introspection helpers are unavailable."
        raise ValueError(msg) from exc
    return introspection_cache_for_ctx(ctx, sql_options=sql_options)


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


def _constraint_name(
    table_name: str,
    *,
    constraint: str,
    index: int,
) -> str:
    digest = hash_sha256_hex(constraint.encode("utf-8"))[:8]
    return f"{table_name}_constraint_{index}_{digest}"


def _parse_constraint_columns(constraint: str) -> tuple[str, ...]:
    match = re.search(r"\((.*?)\)", constraint)
    if match is None:
        return ()
    raw = match.group(1)
    return tuple(token.strip().strip('"') for token in raw.split(",") if token.strip().strip('"'))


def _constraint_type_and_columns(constraint: str) -> tuple[str, tuple[str, ...]]:
    normalized = constraint.strip()
    if not normalized:
        return "CHECK", ()
    upper = normalized.upper()
    if upper.startswith("PRIMARY KEY"):
        return "PRIMARY KEY", _parse_constraint_columns(normalized)
    if upper.startswith("UNIQUE"):
        return "UNIQUE", _parse_constraint_columns(normalized)
    if upper.startswith("CHECK"):
        return "CHECK", ()
    return "CHECK", ()


def _constraint_rows_from_metadata(
    *,
    table_name: str,
    metadata_constraints: Sequence[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, constraint in enumerate(metadata_constraints):
        normalized = constraint.strip()
        if not normalized:
            continue
        constraint_type, columns = _constraint_type_and_columns(normalized)
        constraint_name = _constraint_name(table_name, constraint=normalized, index=index)
        if columns:
            for position, column in enumerate(columns, start=1):
                rows.append(
                    {
                        "table_catalog": None,
                        "table_schema": None,
                        "table_name": table_name,
                        "constraint_name": constraint_name,
                        "constraint_type": constraint_type,
                        "column_name": column,
                        "ordinal_position": position,
                    }
                )
        else:
            rows.append(
                {
                    "table_catalog": None,
                    "table_schema": None,
                    "table_name": table_name,
                    "constraint_name": constraint_name,
                    "constraint_type": constraint_type,
                    "column_name": None,
                    "ordinal_position": None,
                }
            )
    return rows


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
        from datafusion_engine.udf.runtime import rust_udf_snapshot
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
    _ = ctx
    return base


def _merge_registry_parameters(ctx: SessionContext, base: pa.Table | None) -> pa.Table:
    _ = ctx
    return base or _empty_parameters_table()


def _information_schema_table(
    ctx: SessionContext,
    *,
    name: str,
    sql_options: SQLOptions | None,
) -> pa.Table | None:
    panic_exception_type: type[Exception] = RuntimeError
    with contextlib.suppress(ImportError):  # pragma: no cover - optional dependency
        module = importlib.import_module("pyo3_runtime")
        candidate = getattr(module, "PanicException", RuntimeError)
        if isinstance(candidate, type) and issubclass(candidate, Exception):
            panic_exception_type = candidate
    query = f"select * from information_schema.{name}"
    resolved_options = sql_options or _read_only_sql_options()
    try:
        df = ctx.sql_with_options(query, resolved_options)
        return df.to_arrow_table()
    except (RuntimeError, TypeError, ValueError, AttributeError, panic_exception_type):
        return None


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
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    base = snapshot.routines if snapshot.routines is not None else _empty_routines_table()
    if base.num_rows == 0:
        fallback = _information_schema_table(ctx, name="routines", sql_options=sql_options)
        if fallback is not None:
            base = fallback
    return _merge_registry_routines(ctx, base)


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
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    base = getattr(snapshot, "parameters", None)
    if base is None or base.num_rows == 0:
        fallback = _information_schema_table(ctx, name="parameters", sql_options=sql_options)
        if fallback is not None:
            base = fallback
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

    Returns:
    -------
    list[dict[str, object]]
        Rows including constraint type and column names where available.
    """
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    rows = _constraint_rows_from_snapshot(snapshot, table_name=table_name)
    metadata = table_provider_metadata(id(ctx), table_name=table_name)
    if metadata is None or not metadata.constraints:
        return rows
    extra_rows = _constraint_rows_from_metadata(
        table_name=table_name,
        metadata_constraints=metadata.constraints,
    )
    if not rows:
        return extra_rows
    existing = {
        (
            row.get("constraint_type"),
            row.get("constraint_name"),
            row.get("column_name"),
        )
        for row in rows
    }
    for row in extra_rows:
        key = (
            row.get("constraint_type"),
            row.get("constraint_name"),
            row.get("column_name"),
        )
        if key not in existing:
            rows.append(row)
    return rows


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
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return _constraint_rows_from_snapshot(snapshot, catalog=catalog, schema=schema)


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
            df = self.ctx.sql_with_options(sql, self.sql_options or _read_only_sql_options())
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
        metadata = table_provider_metadata(id(self.ctx), table_name=table_name)
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
        metadata = table_provider_metadata(id(self.ctx), table_name=table_name)
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
    "catalogs_snapshot",
    "find_struct_field_keys",
    "parameters_snapshot_table",
    "routines_snapshot_table",
    "schema_contract_from_table",
    "schema_from_table",
    "tables_snapshot_table",
]
