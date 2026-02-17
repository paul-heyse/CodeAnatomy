"""Tests for schema snapshot collector helpers."""

from __future__ import annotations

from datafusion_engine.schema import snapshot_collector


class _Introspector:
    @staticmethod
    def tables_snapshot() -> list[dict[str, object]]:
        return [{"name": "t"}]

    @staticmethod
    def schemata_snapshot() -> list[dict[str, object]]:
        return [{"name": "s"}]

    @staticmethod
    def columns_snapshot() -> list[dict[str, object]]:
        return [{"name": "c"}]

    @staticmethod
    def settings_snapshot() -> list[dict[str, object]]:
        return [{"name": "set"}]


def test_snapshot_collector_delegates() -> None:
    """Snapshot collector helpers should delegate to introspector methods."""
    i = _Introspector()
    assert snapshot_collector.tables_snapshot(i) == [{"name": "t"}]
    assert snapshot_collector.schemata_snapshot(i) == [{"name": "s"}]
    assert snapshot_collector.columns_snapshot(i) == [{"name": "c"}]
    assert snapshot_collector.settings_snapshot(i) == [{"name": "set"}]
