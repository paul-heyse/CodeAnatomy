"""Snapshot collection helpers extracted from SchemaIntrospector."""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.schema.introspection_routines import _introspection_cache_for_ctx


class SnapshotIntrospector(Protocol):
    """Protocol for schema snapshot providers."""

    def tables_snapshot(self) -> list[dict[str, object]]:
        """Return table snapshot rows."""
        ...

    def schemata_snapshot(self) -> list[dict[str, object]]:
        """Return schema snapshot rows."""
        ...

    def columns_snapshot(self) -> list[dict[str, object]]:
        """Return column snapshot rows."""
        ...

    def settings_snapshot(self) -> list[dict[str, object]]:
        """Return settings snapshot rows."""
        ...


def tables_snapshot(introspector: SnapshotIntrospector) -> list[dict[str, object]]:
    """Return table metadata rows from the schema introspector."""
    return introspector.tables_snapshot()


def schemata_snapshot(introspector: SnapshotIntrospector) -> list[dict[str, object]]:
    """Return schema metadata rows from the schema introspector."""
    return introspector.schemata_snapshot()


def columns_snapshot(introspector: SnapshotIntrospector) -> list[dict[str, object]]:
    """Return column metadata rows from the schema introspector."""
    return introspector.columns_snapshot()


def settings_snapshot(introspector: SnapshotIntrospector) -> list[dict[str, object]]:
    """Return settings metadata rows from the schema introspector."""
    return introspector.settings_snapshot()


def settings_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return session settings as a pyarrow.Table."""
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return snapshot.settings


def tables_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return table inventory rows as a pyarrow.Table."""
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    return snapshot.tables


def routines_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return information_schema.routines as a pyarrow.Table."""
    from datafusion_engine.schema.introspection_routines import (
        routines_snapshot_table as _routines_snapshot_table,
    )

    return _routines_snapshot_table(ctx, sql_options=sql_options)


__all__ = [
    "columns_snapshot",
    "routines_snapshot_table",
    "schemata_snapshot",
    "settings_snapshot",
    "settings_snapshot_table",
    "tables_snapshot",
    "tables_snapshot_table",
]
