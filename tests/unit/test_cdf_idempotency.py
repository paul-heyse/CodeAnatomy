"""Unit tests for Delta CDF idempotency and replay scenarios."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.cdf_filters import CdfChangeType, CdfFilterPolicy
from incremental.changes import file_changes_from_cdf
from incremental.diff import diff_snapshots_with_delta_cdf
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from storage.deltalake import DeltaWriteOptions, enable_delta_features, write_table_delta

SNAPSHOT_SIZE_BYTES = 100
SNAPSHOT_MTIME_NS = 1000
VERSION_FIVE = 5
VERSION_THREE = 3
VERSION_SEVEN = 7
EXPECTED_CURSOR_COUNT = 3


def _make_snapshot_table(rows: list[tuple[str, str, str]]) -> pa.Table:
    """Create a snapshot table from file_id, path, sha256 tuples.

    Returns
    -------
        Snapshot table for CDF tests.
    """
    return pa.table(
        {
            "file_id": [row[0] for row in rows],
            "path": [row[1] for row in rows],
            "file_sha256": [row[2] for row in rows],
            "size_bytes": [SNAPSHOT_SIZE_BYTES for _ in rows],
            "mtime_ns": [SNAPSHOT_MTIME_NS for _ in rows],
        }
    )


def test_cdf_cursor_persistence(tmp_path: Path) -> None:
    """Test CDF cursor save and load."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")

    # Save a cursor
    cursor = CdfCursor(dataset_name="test_dataset", last_version=VERSION_FIVE)
    store.save_cursor(cursor)

    # Load it back
    loaded = store.load_cursor("test_dataset")
    assert loaded is not None
    assert loaded.dataset_name == "test_dataset"
    assert loaded.last_version == VERSION_FIVE


def test_cdf_cursor_nonexistent(tmp_path: Path) -> None:
    """Test loading a non-existent cursor."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")
    loaded = store.load_cursor("missing_dataset")
    assert loaded is None


def test_cdf_cursor_update(tmp_path: Path) -> None:
    """Test updating a cursor to a new version."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")

    # Save initial cursor
    cursor_v1 = CdfCursor(dataset_name="test_dataset", last_version=VERSION_THREE)
    store.save_cursor(cursor_v1)

    # Update to new version
    cursor_v2 = CdfCursor(dataset_name="test_dataset", last_version=VERSION_SEVEN)
    store.save_cursor(cursor_v2)

    # Load should return updated version
    loaded = store.load_cursor("test_dataset")
    assert loaded is not None
    assert loaded.last_version == VERSION_SEVEN


def test_cdf_cursor_delete(tmp_path: Path) -> None:
    """Test deleting a cursor."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")

    cursor = CdfCursor(dataset_name="test_dataset", last_version=VERSION_FIVE)
    store.save_cursor(cursor)
    assert store.has_cursor("test_dataset")

    store.delete_cursor("test_dataset")
    assert not store.has_cursor("test_dataset")


def test_cdf_cursor_list(tmp_path: Path) -> None:
    """Test listing all cursors."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")

    # Save multiple cursors
    store.save_cursor(CdfCursor(dataset_name="dataset_a", last_version=1))
    store.save_cursor(CdfCursor(dataset_name="dataset_b", last_version=2))
    store.save_cursor(CdfCursor(dataset_name="dataset_c", last_version=VERSION_THREE))

    cursors = store.list_cursors()
    assert len(cursors) == EXPECTED_CURSOR_COUNT
    dataset_names = {c.dataset_name for c in cursors}
    assert dataset_names == {"dataset_a", "dataset_b", "dataset_c"}


def test_cdf_filter_policy_all() -> None:
    """Test filter policy that includes all change types."""
    policy = CdfFilterPolicy.include_all()
    assert policy.matches(CdfChangeType.INSERT)
    assert policy.matches(CdfChangeType.UPDATE_POSTIMAGE)
    assert policy.matches(CdfChangeType.DELETE)
    assert policy.to_sql_predicate() is None


def test_cdf_filter_policy_inserts_only() -> None:
    """Test filter policy that includes only inserts."""
    policy = CdfFilterPolicy.inserts_only()
    assert policy.matches(CdfChangeType.INSERT)
    assert not policy.matches(CdfChangeType.UPDATE_POSTIMAGE)
    assert not policy.matches(CdfChangeType.DELETE)
    predicate = policy.to_sql_predicate()
    assert predicate is not None
    assert "_change_type = 'insert'" in predicate


def test_cdf_filter_policy_inserts_and_updates() -> None:
    """Test filter policy that excludes deletes."""
    policy = CdfFilterPolicy.inserts_and_updates_only()
    assert policy.matches(CdfChangeType.INSERT)
    assert policy.matches(CdfChangeType.UPDATE_POSTIMAGE)
    assert not policy.matches(CdfChangeType.DELETE)
    predicate = policy.to_sql_predicate()
    assert predicate is not None
    assert "_change_type IN" in predicate


def test_cdf_filter_policy_custom() -> None:
    """Test custom filter policy."""
    policy = CdfFilterPolicy(
        include_insert=False, include_update_postimage=True, include_delete=True
    )
    assert not policy.matches(CdfChangeType.INSERT)
    assert policy.matches(CdfChangeType.UPDATE_POSTIMAGE)
    assert policy.matches(CdfChangeType.DELETE)


def test_cdf_change_type_from_column() -> None:
    """Test converting CDF column values to enum."""
    assert CdfChangeType.from_cdf_column("insert") == CdfChangeType.INSERT
    assert CdfChangeType.from_cdf_column("INSERT") == CdfChangeType.INSERT
    assert CdfChangeType.from_cdf_column("update_postimage") == CdfChangeType.UPDATE_POSTIMAGE
    assert CdfChangeType.from_cdf_column("update_post") == CdfChangeType.UPDATE_POSTIMAGE
    assert CdfChangeType.from_cdf_column("delete") == CdfChangeType.DELETE
    assert CdfChangeType.from_cdf_column("unknown") is None


def test_file_changes_from_cdf_empty() -> None:
    """Test deriving changes from empty CDF table."""
    runtime = _runtime_or_skip()
    cdf_table = pa.table(
        {
            "file_id": pa.array([], type=pa.string()),
            "_change_type": pa.array([], type=pa.string()),
        }
    )
    changes = file_changes_from_cdf(cdf_table, runtime=runtime)
    assert len(changes.changed_file_ids) == 0
    assert len(changes.deleted_file_ids) == 0
    assert not changes.full_refresh


def test_file_changes_from_cdf_inserts() -> None:
    """Test deriving changes from CDF with only inserts."""
    runtime = _runtime_or_skip()
    cdf_table = pa.table(
        {
            "file_id": ["file_a", "file_b", "file_c"],
            "_change_type": ["insert", "insert", "insert"],
            "path": ["a.py", "b.py", "c.py"],
        }
    )
    changes = file_changes_from_cdf(cdf_table, runtime=runtime)
    assert set(changes.changed_file_ids) == {"file_a", "file_b", "file_c"}
    assert len(changes.deleted_file_ids) == 0
    assert not changes.full_refresh


def test_file_changes_from_cdf_deletes() -> None:
    """Test deriving changes from CDF with only deletes."""
    runtime = _runtime_or_skip()
    cdf_table = pa.table(
        {
            "file_id": ["file_x", "file_y"],
            "_change_type": ["delete", "delete"],
            "path": ["x.py", "y.py"],
        }
    )
    changes = file_changes_from_cdf(cdf_table, runtime=runtime)
    assert len(changes.changed_file_ids) == 0
    assert set(changes.deleted_file_ids) == {"file_x", "file_y"}
    assert not changes.full_refresh


def test_file_changes_from_cdf_mixed() -> None:
    """Test deriving changes from CDF with mixed change types."""
    runtime = _runtime_or_skip()
    cdf_table = pa.table(
        {
            "file_id": ["file_a", "file_b", "file_c", "file_d"],
            "_change_type": ["insert", "update_postimage", "delete", "insert"],
            "path": ["a.py", "b.py", "c.py", "d.py"],
        }
    )
    changes = file_changes_from_cdf(cdf_table, runtime=runtime)
    # Inserts and updates are both "changed"
    assert set(changes.changed_file_ids) == {"file_a", "file_b", "file_d"}
    assert set(changes.deleted_file_ids) == {"file_c"}
    assert not changes.full_refresh


def test_file_changes_from_cdf_none() -> None:
    """Test deriving changes from None CDF table."""
    runtime = _runtime_or_skip()
    changes = file_changes_from_cdf(None, runtime=runtime)
    assert len(changes.changed_file_ids) == 0
    assert len(changes.deleted_file_ids) == 0
    assert not changes.full_refresh


def test_cdf_idempotency_replay_same_version(tmp_path: Path) -> None:
    """Test that replaying CDF at same version returns no changes."""
    runtime = _runtime_or_skip()
    dataset_path = tmp_path / "test_dataset"
    dataset_path.mkdir()

    # Write initial version
    initial = _make_snapshot_table([("file_a", "a.py", "sha1")])
    write_table_delta(
        initial,
        str(dataset_path),
        options=DeltaWriteOptions(mode="append"),
    )
    enable_delta_features(str(dataset_path))

    # Set up cursor store
    state_store = StateStore(tmp_path / "state")
    cursor_store = CdfCursorStore(cursors_path=state_store.cdf_cursors_path())

    # First read - should create cursor
    cdf_result = diff_snapshots_with_delta_cdf(
        dataset_path=str(dataset_path),
        cursor_store=cursor_store,
        dataset_name="test_dataset",
        runtime=runtime,
    )
    # First read with no prior cursor returns None
    assert cdf_result is None

    # Second read at same version - should return None
    cdf_result_2 = diff_snapshots_with_delta_cdf(
        dataset_path=str(dataset_path),
        cursor_store=cursor_store,
        dataset_name="test_dataset",
        runtime=runtime,
    )
    assert cdf_result_2 is None


def test_cdf_idempotency_no_duplicate_processing(tmp_path: Path) -> None:
    """Test that processed versions are not reprocessed."""
    runtime = _runtime_or_skip()
    dataset_path = tmp_path / "test_dataset"
    dataset_path.mkdir()

    # Write version 0
    v0 = _make_snapshot_table([("file_a", "a.py", "sha1")])
    write_table_delta(
        v0,
        str(dataset_path),
        options=DeltaWriteOptions(mode="append"),
    )
    enable_delta_features(str(dataset_path))

    # Write version 1
    v1 = _make_snapshot_table([("file_b", "b.py", "sha2")])
    write_table_delta(
        v1,
        str(dataset_path),
        options=DeltaWriteOptions(mode="append"),
    )

    # Set up cursor store
    state_store = StateStore(tmp_path / "state")
    cursor_store = CdfCursorStore(cursors_path=state_store.cdf_cursors_path())

    # First read - creates cursor at v1
    cdf_result = diff_snapshots_with_delta_cdf(
        dataset_path=str(dataset_path),
        cursor_store=cursor_store,
        dataset_name="test_dataset",
        runtime=runtime,
    )
    assert cdf_result is None  # First read returns None

    # Check cursor was created
    cursor = cursor_store.load_cursor("test_dataset")
    assert cursor is not None
    assert cursor.last_version == 1

    # Second read - should return None since no new versions
    cdf_result_2 = diff_snapshots_with_delta_cdf(
        dataset_path=str(dataset_path),
        cursor_store=cursor_store,
        dataset_name="test_dataset",
        runtime=runtime,
    )
    assert cdf_result_2 is None


def test_state_store_cdf_cursors_path(tmp_path: Path) -> None:
    """Test StateStore provides CDF cursors path."""
    store = StateStore(tmp_path)
    cursors_path = store.cdf_cursors_path()
    assert cursors_path == tmp_path / "metadata" / "cdf_cursors"


def _runtime_or_skip() -> IncrementalRuntime:
    try:
        runtime = IncrementalRuntime.build()
        _ = runtime.ibis_backend()
    except ImportError as exc:
        pytest.skip(str(exc))
    else:
        return runtime
    msg = "Incremental runtime unavailable."
    raise RuntimeError(msg)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
