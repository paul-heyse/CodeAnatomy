"""Delta-focused schema introspection helpers."""

from __future__ import annotations

from datafusion import SessionContext, SQLOptions

from datafusion_engine.schema.introspection_core import (
    _constraint_rows_from_metadata,
    _constraint_rows_from_snapshot,
    _introspection_cache_for_ctx,
)
from datafusion_engine.tables.metadata import table_provider_metadata


def table_constraint_rows(
    ctx: SessionContext,
    *,
    table_name: str,
    sql_options: SQLOptions | None = None,
) -> list[dict[str, object]]:
    """Return table-level constraint rows from information_schema."""
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
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
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return _constraint_rows_from_snapshot(snapshot, catalog=catalog, schema=schema)


__all__ = [
    "constraint_rows",
    "table_constraint_rows",
]
