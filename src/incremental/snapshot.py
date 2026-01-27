"""Snapshot helpers for incremental pipeline runs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.schema.abi import schema_fingerprint
from arrowdsl.schema.build import column_or_null, table_from_arrays
from datafusion_engine.runtime import dataset_schema_from_context
from incremental.delta_context import DeltaAccessContext, read_delta_table_via_facade
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    build_commit_properties,
    delta_table_version,
    enable_delta_features,
    open_delta_table,
    write_delta_table,
)

if TYPE_CHECKING:
    from arrowdsl.core.interop import TableLike
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


def read_repo_snapshot(
    store: StateStore,
    *,
    context: DeltaAccessContext,
) -> pa.Table | None:
    """Load the previous repo snapshot when present.

    Returns
    -------
    pa.Table | None
        Snapshot table if it exists.
    """
    path = store.repo_snapshot_path()
    if not path.exists():
        return None
    storage = context.storage
    version = delta_table_version(
        str(path),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
    if version is None:
        return None
    return _read_delta_table(context, path, name="repo_snapshot_read")


def write_repo_snapshot(
    store: StateStore,
    snapshot: pa.Table,
    *,
    context: DeltaAccessContext,
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
    storage = context.storage
    existing_version = delta_table_version(
        str(target),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
    if existing_version is None:
        result = write_delta_table(
            snapshot,
            str(target),
            options=DeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=metadata,
                storage_options=storage.storage_options,
                log_storage_options=storage.log_storage_options,
            ),
        )
        enable_delta_features(
            result.path,
            storage_options=storage.storage_options,
            log_storage_options=storage.log_storage_options,
        )
        return result
    commit_key = str(target)
    commit_options, commit_run = context.runtime.profile.reserve_delta_commit(
        key=commit_key,
        metadata={"dataset": commit_key, "operation": "merge"},
        commit_metadata=metadata,
    )
    commit_properties = build_commit_properties(
        app_id=commit_options.app_id,
        version=commit_options.version,
        commit_metadata=metadata,
    )
    table = open_delta_table(
        str(target),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
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
    context.runtime.profile.finalize_delta_commit(
        key=commit_key,
        run=commit_run,
        metadata={"operation": "merge", "rows_affected": snapshot.num_rows},
    )
    enable_delta_features(
        str(target),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
    return DeltaWriteResult(
        path=str(target),
        version=delta_table_version(
            str(target),
            storage_options=storage.storage_options,
            log_storage_options=storage.log_storage_options,
        ),
    )


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
    *,
    name: str,
) -> pa.Table:
    return read_delta_table_via_facade(context, path=path, name=name)


__all__ = ["build_repo_snapshot", "read_repo_snapshot", "write_repo_snapshot"]
