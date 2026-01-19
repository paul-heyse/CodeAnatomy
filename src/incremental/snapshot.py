"""Snapshot helpers for incremental pipeline runs."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import column_or_null, table_from_arrays
from arrowdsl.schema.serialization import schema_fingerprint
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    delta_table_version,
    enable_delta_features,
    read_table_delta,
    write_table_delta,
)


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
    if not path.exists() or delta_table_version(str(path)) is None:
        return None
    return cast("pa.Table", read_table_delta(str(path)))


def write_repo_snapshot(store: StateStore, snapshot: pa.Table) -> DeltaWriteResult:
    """Persist the repo snapshot to the state store as Delta.

    Returns
    -------
    str
        Path to the written snapshot Delta table.
    """
    store.ensure_dirs()
    target = store.repo_snapshot_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "snapshot_kind": "repo_snapshot",
        "schema_fingerprint": schema_fingerprint(snapshot.schema),
    }
    existing_version = delta_table_version(str(target))
    if existing_version is None:
        result = write_table_delta(
            snapshot,
            str(target),
            options=DeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=metadata,
            ),
        )
        enable_delta_features(result.path)
        return result
    table = DeltaTable(str(target))
    update_predicate = (
        "source.file_sha256 <> target.file_sha256 OR "
        "source.path <> target.path OR "
        "source.size_bytes <> target.size_bytes OR "
        "source.mtime_ns <> target.mtime_ns"
    )
    (
        table.merge(
            snapshot,
            predicate="source.file_id = target.file_id",
            source_alias="source",
            target_alias="target",
            merge_schema=True,
            commit_properties=CommitProperties(custom_metadata=metadata),
        )
        .when_matched_update_all(predicate=update_predicate)
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    enable_delta_features(str(target))
    return DeltaWriteResult(
        path=str(target),
        version=delta_table_version(str(target)),
    )


__all__ = ["build_repo_snapshot", "read_repo_snapshot", "write_repo_snapshot"]
