"""Tests for DF52 cache snapshot helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.cache import metadata_snapshots

_ENTRY_COUNT = 4
_ROW_ENTRY_COUNT = 7


def test_cache_snapshot_rows_maps_by_cache_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Snapshot rows are indexed by normalized cache name."""
    cache_snapshot_rows = metadata_snapshots.__dict__["_cache_snapshot_rows"]

    def _capture(_ctx: object) -> dict[str, object]:
        return {
            "cache_snapshots": [
                {"cache_name": "metadata", "entry_count": _ENTRY_COUNT},
                {"cache_name": "statistics", "entry_count": 9},
            ]
        }

    monkeypatch.setattr(
        "datafusion_engine.catalog.introspection.capture_cache_diagnostics",
        _capture,
    )
    rows = cache_snapshot_rows(object())
    assert set(rows) == {"metadata", "statistics"}
    assert rows["metadata"]["entry_count"] == _ENTRY_COUNT


def test_cache_snapshot_source_table_uses_row_metrics() -> None:
    """Source table carries cache metrics into Arrow fields."""
    cache_snapshot_source_table = metadata_snapshots.__dict__["_cache_snapshot_source_table"]
    table = cache_snapshot_source_table(
        "list_files",
        row={
            "event_time_unix_ms": 123,
            "entry_count": _ROW_ENTRY_COUNT,
            "hit_count": 3,
            "miss_count": 2,
            "eviction_count": 1,
            "config_ttl": "2m",
            "config_limit": "64",
        },
    )
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column("cache_name")[0].as_py() == "list_files"
    assert table.column("entry_count")[0].as_py() == _ROW_ENTRY_COUNT
