"""Unit tests for Delta-backed incremental snapshots."""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.schema.build import rows_from_table
from datafusion_engine.registry_bridge import (
    DeltaCdfRegistrationOptions,
    register_delta_cdf_df,
)
from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.diff import diff_snapshots_with_cdf
from incremental.snapshot import write_repo_snapshot
from incremental.state_store import StateStore
from storage.deltalake import DeltaCdfOptions


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


@contextlib.contextmanager
def _cdf_table_ref(
    path: str,
    options: DeltaCdfOptions,
) -> Generator[tuple[SessionContext, str]]:
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    table_name = f"cdf_{uuid4().hex}"
    df = register_delta_cdf_df(
        ctx,
        name=table_name,
        path=path,
        options=DeltaCdfRegistrationOptions(
            cdf_options=options,
            runtime_profile=profile,
        ),
    )
    _ = df
    try:
        yield ctx, table_name
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)


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

    cdf_options = DeltaCdfOptions(
        starting_version=1,
        ending_version=result.version,
        allow_out_of_range=True,
    )
    with _cdf_table_ref(str(store.repo_snapshot_path()), cdf_options) as (cdf_ctx, cdf_name):
        diff = diff_snapshots_with_cdf(
            snapshot_one,
            snapshot_two,
            ctx=cdf_ctx,
            cdf_table=cdf_name,
        )
    change_map = {row["file_id"]: row["change_kind"] for row in rows_from_table(diff)}
    assert change_map["file_a"] == "modified"
    assert change_map["file_b"] == "deleted"
    assert change_map["file_c"] == "added"
