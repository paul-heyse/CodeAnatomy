"""Unit tests for Delta-backed incremental snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from arrowdsl.schema.build import rows_from_table
from incremental.diff import diff_snapshots_with_cdf
from incremental.snapshot import write_repo_snapshot
from incremental.state_store import StateStore
from storage.deltalake import DeltaCdfOptions, read_delta_cdf


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
    write_repo_snapshot(store, snapshot_one)

    snapshot_two = _snapshot_table(
        [
            ("file_a", "src/a.py", "sha3", 12, 120),
            ("file_c", "src/c.py", "sha4", 30, 300),
        ]
    )
    result = write_repo_snapshot(store, snapshot_two)
    assert result.version is not None

    cdf = read_delta_cdf(
        str(store.repo_snapshot_path()),
        options=DeltaCdfOptions(
            starting_version=1,
            ending_version=result.version,
            allow_out_of_range=True,
        ),
    )
    diff = diff_snapshots_with_cdf(snapshot_one, snapshot_two, cdf)
    change_map = {row["file_id"]: row["change_kind"] for row in rows_from_table(diff)}
    assert change_map["file_a"] == "modified"
    assert change_map["file_b"] == "deleted"
    assert change_map["file_c"] == "added"
