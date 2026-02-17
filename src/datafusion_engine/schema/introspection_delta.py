"""Delta-focused schema introspection helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from datafusion import SessionContext, SQLOptions

from datafusion_engine.catalog.introspection import introspection_cache_for_ctx
from datafusion_engine.tables.metadata import table_provider_metadata
from utils.hashing import hash_sha256_hex


def _normalized_rows(table: object) -> list[dict[str, object]]:
    to_pylist = getattr(table, "to_pylist", None)
    if not callable(to_pylist):
        return []
    rows = to_pylist()
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append({str(key).lower(): value for key, value in row.items()})
    return normalized


def _constraint_rows_from_snapshot(
    snapshot: object,
    *,
    table_name: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
) -> list[dict[str, object]]:
    table_constraints = getattr(snapshot, "table_constraints", None)
    key_column_usage = getattr(snapshot, "key_column_usage", None)
    if table_constraints is None or key_column_usage is None:
        return []
    constraints = _normalized_rows(table_constraints)
    usage_rows = _normalized_rows(key_column_usage)
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


def table_constraint_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return table-level constraint rows from information_schema."""
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    rows = _constraint_rows_from_snapshot(snapshot, table_name=table_name)
    metadata = table_provider_metadata(ctx, table_name=table_name)
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
    """Return normalized constraint rows for a table."""
    snapshot = introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return _constraint_rows_from_snapshot(snapshot, catalog=catalog, schema=schema)


__all__ = [
    "constraint_rows",
    "table_constraint_rows",
]
