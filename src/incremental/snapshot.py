"""Snapshot helpers for incremental pipeline runs."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
from deltalake import DeltaTable

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import column_or_null, table_from_arrays
from arrowdsl.schema.serialization import schema_fingerprint
from datafusion_engine.runtime import dataset_schema_from_context, read_delta_as_reader
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaWriteResult,
    build_commit_properties,
    delta_table_version,
    enable_delta_features,
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
    schema = dataset_schema_from_context("repo_snapshot_v1")
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
    reader = read_delta_as_reader(str(path))
    return reader.read_all()


def write_repo_snapshot(
    store: StateStore,
    snapshot: pa.Table,
    *,
    runtime: IncrementalRuntime,
) -> DeltaWriteResult:
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
        result = write_ibis_dataset_delta(
            snapshot,
            str(target),
            options=IbisDatasetWriteOptions(
                execution=runtime.ibis_execution(),
                writer_strategy="datafusion",
                delta_options=IbisDeltaWriteOptions(
                    mode="overwrite",
                    schema_mode="overwrite",
                    commit_metadata=metadata,
                ),
            ),
        )
        enable_delta_features(result.path)
        return result
    commit_key = str(target)
    commit_options, commit_run = runtime.profile.reserve_delta_commit(
        key=commit_key,
        metadata={"dataset": commit_key, "operation": "merge"},
        commit_metadata=metadata,
    )
    commit_properties = build_commit_properties(
        app_id=commit_options.app_id,
        version=commit_options.version,
        commit_metadata=metadata,
    )
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
            commit_properties=commit_properties,
        )
        .when_matched_update_all(predicate=update_predicate)
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    runtime.profile.finalize_delta_commit(
        key=commit_key,
        run=commit_run,
        metadata={"operation": "merge", "rows_affected": snapshot.num_rows},
    )
    enable_delta_features(str(target))
    return DeltaWriteResult(
        path=str(target),
        version=delta_table_version(str(target)),
    )


__all__ = ["build_repo_snapshot", "read_repo_snapshot", "write_repo_snapshot"]
