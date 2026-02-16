"""Integration tests for CDF cursor lifecycle with Delta CDF runtime.

Suite 3.12: CDF Cursor Lifecycle integration tests.

This test suite validates cursor <-> Delta/CDF runtime interaction through
real CDF reads and writes. Focuses on state progression, recovery, and
concurrent updates. Unit tests already cover API fundamentals like
`get_start_version` and path sanitization.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_reader import CdfReadOptions, read_cdf_changes
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

require_datafusion_udfs()
require_deltalake()
require_delta_extension()

START_VERSION_AFTER_TWO_READS = 2
INITIAL_CURSOR_VERSION = 5
RECOVERY_CURSOR_VERSION = 10
UPDATED_CURSOR_VERSION = 15


def _write_delta_table_with_cdf(delta_path: Path, table: pa.Table) -> int:
    """Write a Delta table with CDF enabled.

    Parameters
    ----------
    delta_path
        Path where Delta table will be written.
    table
        PyArrow table to write.

    Returns:
    -------
    int
        Version number of written Delta table.
    """
    from deltalake import write_deltalake

    write_deltalake(
        str(delta_path),
        table,
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    return 0


def _append_to_delta_table(delta_path: Path, table: pa.Table) -> int:
    """Append to existing Delta table.

    Parameters
    ----------
    delta_path
        Path to the Delta table.
    table
        PyArrow table to append.

    Returns:
    -------
    int
        Version number after append.
    """
    from deltalake import DeltaTable

    dt = DeltaTable(str(delta_path))
    version_before = dt.version()
    dt.merge(
        source=table,
        predicate="1=0",  # Never match - forces insert-only
        source_alias="source",
        target_alias="target",
    ).when_not_matched_insert_all().execute()
    dt.update_incremental()
    return version_before + 1


def _create_session() -> tuple[DataFusionRuntimeProfile, SessionContext]:
    """Create a DataFusion session with runtime profile.

    Returns:
    -------
    tuple[DataFusionRuntimeProfile, SessionContext]
        Runtime profile and session context.
    """
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    return profile, ctx


def _verify_first_read(
    store: CdfCursorStore,
    dataset_name: str,
    delta_path: Path,
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> CdfCursor:
    """Verify first CDF read with no existing cursor.

    Parameters
    ----------
    store
        Cursor store.
    dataset_name
        Dataset name.
    delta_path
        Path to Delta table.
    ctx
        DataFusion session context.
    profile
        Runtime profile.

    Returns:
    -------
    CdfCursor
        Cursor after first read.
    """
    start_version = store.get_start_version(dataset_name)
    assert start_version is None, "No cursor should exist initially"

    options = CdfReadOptions(
        cursor_store=store,
        dataset_name=dataset_name,
        runtime_profile=profile,
    )
    result = read_cdf_changes(ctx, str(delta_path), options)

    assert result is not None, "First CDF read should succeed"
    assert result.start_version == 0, "First read should start at version 0"
    assert result.end_version == 0, "First read should end at version 0"
    assert result.has_changes, "Initial data should be present"

    cursor = store.load_cursor(dataset_name)
    assert cursor is not None, "Cursor should be created after first read"
    assert cursor.last_version == 0, "Cursor should track version 0"
    assert cursor.last_timestamp is not None, "Cursor should have timestamp"
    return cursor


def _verify_second_read(
    store: CdfCursorStore,
    dataset_name: str,
    delta_path: Path,
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    first_cursor: CdfCursor,
) -> CdfCursor:
    """Verify second CDF read with existing cursor.

    Parameters
    ----------
    store
        Cursor store.
    dataset_name
        Dataset name.
    delta_path
        Path to Delta table.
    ctx
        DataFusion session context.
    profile
        Runtime profile.
    first_cursor
        Cursor from first read.

    Returns:
    -------
    CdfCursor
        Cursor after second read.
    """
    start_version = store.get_start_version(dataset_name)
    assert start_version == 1, "Start version should be last_version + 1"

    options = CdfReadOptions(
        cursor_store=store,
        dataset_name=dataset_name,
        runtime_profile=profile,
    )
    result = read_cdf_changes(ctx, str(delta_path), options)

    assert result is not None, "Second CDF read should succeed"
    assert result.start_version == 1, "Second read should start at version 1"
    assert result.end_version == 1, "Second read should end at version 1"
    assert result.has_changes, "New changes should be present"

    cursor = store.load_cursor(dataset_name)
    assert cursor is not None, "Cursor should still exist"
    assert cursor.last_version == 1, "Cursor should track version 1"
    assert cursor.last_timestamp is not None, "Cursor should have new timestamp"
    assert cursor.last_timestamp != first_cursor.last_timestamp, "Timestamp should be updated"
    return cursor


@pytest.mark.integration
def test_cursor_state_progression_through_cdf_reads(tmp_path: Path) -> None:
    """Cursor state progresses correctly through multiple CDF reads.

    Test validates that:
    1. First read starts from `get_start_version()` (None -> version 0)
    2. Cursor updates to end_version after successful read
    3. Second read starts from `get_start_version()` (last_version + 1)
    4. Cursor updates again after second read
    5. Version progression matches actual Delta commits
    """
    # Setup: Create cursor store
    cursors_path = tmp_path / "cursors"
    store = CdfCursorStore(cursors_path=cursors_path)
    dataset_name = "test_dataset"

    # Setup: Write initial Delta table with CDF enabled
    delta_path = tmp_path / "delta_table"
    initial_table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    version0 = _write_delta_table_with_cdf(delta_path, initial_table)
    assert version0 == 0

    # Setup: Create session
    profile, ctx = _create_session()

    # First read: No cursor exists, should start from version 0
    cursor1 = _verify_first_read(store, dataset_name, delta_path, ctx, profile)

    # Second write: Append more data to create version 1
    append_table = pa.table({"id": [4, 5], "value": ["d", "e"]})
    version1 = _append_to_delta_table(delta_path, append_table)
    assert version1 == 1, "Append should create version 1"

    # Second read: Cursor exists, should start from version 1 (last_version + 1)
    _verify_second_read(store, dataset_name, delta_path, ctx, profile, cursor1)

    # Third read: No new data, should return None or empty result
    start_version_3 = store.get_start_version(dataset_name)
    assert start_version_3 == START_VERSION_AFTER_TWO_READS, (
        "Start version should be last_version + 1"
    )

    # Read with no new changes
    options3 = CdfReadOptions(
        cursor_store=store,
        dataset_name=dataset_name,
        runtime_profile=profile,
    )
    result3 = read_cdf_changes(ctx, str(delta_path), options3)

    # Verify third read handles no-changes case
    if result3 is not None:
        # If result is returned, it should indicate no changes
        assert not result3.has_changes, "No new changes should be present"
        # Cursor might still be at version 1 if no changes
        cursor3 = store.load_cursor(dataset_name)
        assert cursor3 is not None
        # Version should be at least 1, possibly updated to current table version
        assert cursor3.last_version >= 1


@pytest.mark.integration
def test_cursor_recovery_after_corruption(tmp_path: Path) -> None:
    """Cursor store recovers from corrupted cursor files.

    Test validates that:
    1. Invalid JSON in cursor file returns None on load
    2. Recovery path works by re-establishing cursor
    3. New cursor can be saved and loaded correctly
    4. Cursor operations continue normally after recovery
    """
    # Setup: Create cursor store
    cursors_path = tmp_path / "cursors"
    store = CdfCursorStore(cursors_path=cursors_path)
    dataset_name = "corrupted_dataset"

    # Setup: Write a valid cursor first
    cursor_before = CdfCursor.create(dataset_name, INITIAL_CURSOR_VERSION)
    store.save_cursor(cursor_before)

    # Verify cursor was saved correctly
    loaded_before = store.load_cursor(dataset_name)
    assert loaded_before is not None
    assert loaded_before.last_version == INITIAL_CURSOR_VERSION

    # Corrupt the cursor file by writing invalid JSON
    # Use direct path construction to simulate corruption
    safe_name = dataset_name.replace("/", "_").replace("\\", "_")
    cursor_file = cursors_path / f"{safe_name}.cursor.json"
    cursor_file.write_text("{ invalid json content }")

    # Verify load_cursor returns None for corrupted file
    loaded_corrupted = store.load_cursor(dataset_name)
    assert loaded_corrupted is None, "Corrupted cursor should return None"

    # Verify has_cursor still returns True (file exists)
    assert store.has_cursor(dataset_name), "Cursor file should still exist"

    # Recovery path: Re-establish cursor with new valid data
    recovery_cursor = CdfCursor.create(dataset_name, RECOVERY_CURSOR_VERSION)
    store.save_cursor(recovery_cursor)

    # Verify recovery succeeded
    loaded_after = store.load_cursor(dataset_name)
    assert loaded_after is not None, "Recovered cursor should load"
    assert loaded_after.last_version == RECOVERY_CURSOR_VERSION, (
        "Recovered cursor should have new version"
    )

    # Verify normal operations continue
    updated = store.update_version(dataset_name, UPDATED_CURSOR_VERSION)
    assert updated.last_version == UPDATED_CURSOR_VERSION

    final = store.load_cursor(dataset_name)
    assert final is not None
    assert final.last_version == UPDATED_CURSOR_VERSION


@pytest.mark.integration
@pytest.mark.serial
def test_concurrent_cursor_updates(tmp_path: Path) -> None:
    """Concurrent cursor updates result in valid final state (last write wins).

    Test validates that:
    1. Multiple threads can update same cursor simultaneously
    2. Final state is valid and matches one of the updates
    3. No crashes or corruption occur
    4. Last writer's version persists (no interleaving corruption)
    """
    # Setup: Create cursor store
    cursors_path = tmp_path / "cursors"
    store = CdfCursorStore(cursors_path=cursors_path)
    dataset_name = "concurrent_dataset"

    # Track results from each thread
    results: list[CdfCursor] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def update_cursor(version: int) -> None:
        """Update cursor to specific version.

        Parameters
        ----------
        version
            Version to update to.
        """
        try:
            cursor = store.update_version(dataset_name, version)
            with lock:
                results.append(cursor)
        except Exception as e:  # noqa: BLE001
            with lock:
                errors.append(e)

    # Create multiple threads that update the cursor concurrently
    threads = []
    versions = [10, 20, 30, 40, 50]
    for version in versions:
        thread = threading.Thread(target=update_cursor, args=(version,))
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Verify no errors occurred
    assert len(errors) == 0, f"Concurrent updates should not raise errors: {errors}"

    # Verify all threads completed successfully
    assert len(results) == len(versions), "All threads should complete their updates"

    # Load final cursor state
    final_cursor = store.load_cursor(dataset_name)
    assert final_cursor is not None, "Cursor should exist after concurrent updates"

    # Verify final cursor has one of the written versions (last write wins)
    assert final_cursor.last_version in versions, (
        f"Final version {final_cursor.last_version} should be one of {versions}"
    )

    # Verify cursor file is valid JSON (not corrupted by interleaving)
    # Use direct path construction since we're testing internal state
    safe_name = dataset_name.replace("/", "_").replace("\\", "_")
    cursor_file = cursors_path / f"{safe_name}.cursor.json"
    cursor_bytes = cursor_file.read_bytes()
    try:
        parsed = json.loads(cursor_bytes)
        assert isinstance(parsed, dict), "Cursor file should be valid JSON object"
        assert "dataset_name" in parsed, "Cursor should have dataset_name field"
        assert "last_version" in parsed, "Cursor should have last_version field"
    except json.JSONDecodeError as e:
        pytest.fail(f"Cursor file should be valid JSON, got decode error: {e}")

    # Verify the cursor is still usable for subsequent operations
    next_version = store.get_start_version(dataset_name)
    assert next_version is not None, "get_start_version should work"
    assert next_version == final_cursor.last_version + 1, "next version should be last_version + 1"


__all__ = [
    "test_concurrent_cursor_updates",
    "test_cursor_recovery_after_corruption",
    "test_cursor_state_progression_through_cdf_reads",
]
