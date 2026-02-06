"""Unit tests for Delta-backed incremental snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from datafusion_engine.session.runtime import DataFusionRuntimeProfile, FeatureGatesConfig
from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_runtime import read_cdf_changes
from semantics.incremental.changes import file_changes_from_cdf
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.snapshot import write_repo_snapshot
from semantics.incremental.state_store import StateStore
from storage.deltalake import canonical_table_uri
from tests.test_helpers.optional_deps import require_delta_extension, require_deltalake

require_deltalake()
require_delta_extension()


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
    runtime = _runtime_or_skip()
    context = DeltaAccessContext(runtime)
    store = StateStore(tmp_path)
    snapshot_one = _snapshot_table(
        [
            ("file_a", "src/a.py", "sha1", 10, 100),
            ("file_b", "src/b.py", "sha2", 20, 200),
        ]
    )
    result_one = write_repo_snapshot(store, snapshot_one, context=context)
    assert result_one.version is not None
    assert result_one.snapshot_key is not None
    assert result_one.snapshot_key.version == result_one.version
    assert result_one.snapshot_key.canonical_uri == canonical_table_uri(
        str(store.repo_snapshot_path())
    )

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
    result_two = write_repo_snapshot(store, snapshot_two, context=context)
    assert result_two.version is not None
    assert result_two.snapshot_key is not None
    assert result_two.snapshot_key.version == result_two.version
    assert result_two.snapshot_key.canonical_uri == canonical_table_uri(
        str(store.repo_snapshot_path())
    )

    cdf_result = read_cdf_changes(
        context,
        dataset_path=str(store.repo_snapshot_path()),
        cursor_store=cursor_store,
        dataset_name="repo_snapshot",
        filter_policy=None,
    )
    assert cdf_result is not None
    changes = file_changes_from_cdf(cdf_result, runtime=runtime)
    assert changes.full_refresh is False
    assert changes.changed_file_ids == ("file_a", "file_c")
    assert changes.deleted_file_ids == ("file_b",)


def _runtime_or_skip() -> IncrementalRuntime:
    runtime = IncrementalRuntime.build(
        profile=DataFusionRuntimeProfile(
            features=FeatureGatesConfig(enforce_delta_ffi_provider=False),
        )
    )
    _ = runtime.session_context()
    return runtime
