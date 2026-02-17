"""Snapshot collection helpers extracted from SchemaIntrospector."""

from __future__ import annotations

from typing import Protocol


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


__all__ = [
    "columns_snapshot",
    "schemata_snapshot",
    "settings_snapshot",
    "tables_snapshot",
]
