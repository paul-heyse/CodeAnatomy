"""Snapshot helpers for incremental pipeline runs."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from arrowdsl.core.interop import TableLike
from arrowdsl.io.parquet import read_table_parquet, write_table_parquet
from arrowdsl.schema.build import column_or_null, table_from_arrays
from incremental.state_store import StateStore


def build_repo_snapshot(repo_files: TableLike) -> pa.Table:
    """Return the minimal repo snapshot table.

    Columns:
      - file_id
      - path
      - file_sha256
      - size_bytes
      - mtime_ns

    Returns
    -------
    pa.Table
        Normalized snapshot table.
    """
    table = cast("pa.Table", repo_files)
    schema = pa.schema(
        [
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("file_sha256", pa.string()),
            pa.field("size_bytes", pa.int64()),
            pa.field("mtime_ns", pa.int64()),
        ]
    )
    return table_from_arrays(
        schema,
        columns={
            "file_id": column_or_null(table, "file_id", pa.string()),
            "path": column_or_null(table, "path", pa.string()),
            "file_sha256": column_or_null(table, "file_sha256", pa.string()),
            "size_bytes": column_or_null(table, "size_bytes", pa.int64()),
            "mtime_ns": column_or_null(table, "mtime_ns", pa.int64()),
        },
        num_rows=table.num_rows,
    )


def read_repo_snapshot(store: StateStore) -> pa.Table | None:
    """Load the previous repo snapshot when present.

    Returns
    -------
    pa.Table | None
        Snapshot table if it exists.
    """
    path = store.repo_snapshot_path()
    if not path.exists():
        return None
    return cast("pa.Table", read_table_parquet(path))


def write_repo_snapshot(store: StateStore, snapshot: pa.Table) -> str:
    """Persist the repo snapshot to the state store.

    Returns
    -------
    str
        Path to the written snapshot parquet file.
    """
    store.ensure_dirs()
    target = store.repo_snapshot_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    return write_table_parquet(snapshot, target, overwrite=True)


__all__ = ["build_repo_snapshot", "read_repo_snapshot", "write_repo_snapshot"]
