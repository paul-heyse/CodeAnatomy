"""Unit tests for Delta-backed incremental snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.changes import file_changes_from_cdf
from incremental.diff import diff_snapshots_with_delta_cdf
from incremental.snapshot import write_repo_snapshot
from incremental.state_store import StateStore


def _snapshot_table(rows: list[tuple[str, str, str, int, int]]) -> pa.Table:
    return pa.table(
        {
            "file_id": [row[0] for row in rows],
            "path": [row[1] for row in rows],
            "file_sha256": [row[2] for row in rows],
            "size_bytes": [row[3] for row in rows],
            "mtime_ns": [row[4] for row in rows],
        }
    )


def test_repo_snapshot_cdf_diff(tmp_path: Path) -> None:
    """Compare snapshot diffs using Delta change data feed."""
    store = StateStore(tmp_path)
    snapshot_one = _snapshot_table(
        [
            ("file_a", "src/a.py", "sha1", 10, 100),
            ("file_b", "src/b.py", "sha2", 20, 200),
        ]
    )
    result_one = write_repo_snapshot(store, snapshot_one)
    assert result_one.version is not None

    snapshot_two = _snapshot_table(
        [
            ("file_a", "src/a.py", "sha3", 12, 120),
            ("file_c", "src/c.py", "sha4", 30, 300),
        ]
    )
    cursor_store = CdfCursorStore(cursors_path=store.cdf_cursors_path())
    cursor_store.save_cursor(
        CdfCursor(dataset_name="repo_snapshot", last_version=result_one.version)
    )
    result_two = write_repo_snapshot(store, snapshot_two)
    assert result_two.version is not None

    cdf_table = diff_snapshots_with_delta_cdf(
        dataset_path=str(store.repo_snapshot_path()),
        cursor_store=cursor_store,
        dataset_name="repo_snapshot",
        filter_policy=None,
    )
    assert cdf_table is not None
    changes = file_changes_from_cdf(cdf_table)
    assert changes.full_refresh is False
    assert changes.changed_file_ids == ("file_a", "file_c")
    assert changes.deleted_file_ids == ("file_b",)
