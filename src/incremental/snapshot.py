"""Snapshot helpers for incremental pipeline runs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrow_utils.schema.abi import schema_fingerprint
from arrow_utils.schema.build import column_or_null, table_from_columns
from datafusion_engine.dataset_registry import resolve_delta_constraints
from datafusion_engine.runtime import dataset_schema_from_context
from datafusion_engine.write_pipeline import WriteMode
from incremental.delta_context import DeltaAccessContext, read_delta_table_via_facade
from incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)
from storage.deltalake import (
    DeltaWriteResult,
    delta_merge_arrow,
    delta_table_version,
    idempotent_commit_properties,
)
from storage.deltalake.delta import DeltaFeatureMutationOptions, enable_delta_features

if TYPE_CHECKING:
    from arrow_utils.core.interop import TableLike
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
    return table_from_columns(
        schema,
        {
            "file_id": column_or_null(table, "file_id", pa.string()),
            "path": column_or_null(table, "path", pa.string()),
            "file_sha256": column_or_null(table, "file_sha256", pa.string()),
            "size_bytes": column_or_null(table, "size_bytes", pa.int64()),
            "mtime_ns": column_or_null(table, "mtime_ns", pa.int64()),
        },
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
    storage = context.resolve_storage(table_uri=str(path))
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
    DeltaWriteResult
        Delta write result for the snapshot table.

    Raises
    ------
    RuntimeError
        Raised when the Delta write result is unavailable.
    """
    store.ensure_dirs()
    target = store.repo_snapshot_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "snapshot_kind": "repo_snapshot",
        "schema_fingerprint": schema_fingerprint(snapshot.schema),
        "dataset": str(target),
    }
    storage = context.resolve_storage(table_uri=str(target))
    existing_version = delta_table_version(
        str(target),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
    if existing_version is None:
        commit_metadata = {
            **metadata,
            "operation": "snapshot_overwrite",
            "mode": "overwrite",
        }
        write_result = write_delta_table_via_pipeline(
            runtime=context.runtime,
            table=snapshot,
            request=IncrementalDeltaWriteRequest(
                destination=str(target),
                mode=WriteMode.OVERWRITE,
                schema_mode="overwrite",
                commit_metadata=commit_metadata,
                storage_options=storage.storage_options,
                log_storage_options=storage.log_storage_options,
                operation_id="incremental_snapshot::repo_snapshot",
            ),
        )
        if write_result.delta_result is None:
            msg = "Repo snapshot Delta write did not return a result."
            raise RuntimeError(msg)
        return write_result.delta_result
    commit_key = str(target)
    commit_metadata = {
        **metadata,
        "operation": "snapshot_merge",
        "mode": "merge",
    }
    commit_options, commit_run = context.runtime.profile.reserve_delta_commit(
        key=commit_key,
        metadata=commit_metadata,
        commit_metadata=commit_metadata,
    )
    commit_properties = idempotent_commit_properties(
        operation="snapshot_merge",
        mode="merge",
        idempotent=commit_options,
        extra_metadata=commit_metadata,
    )
    update_predicate = (
        "source.file_sha256 <> target.file_sha256 OR "
        "source.path <> target.path OR "
        "source.size_bytes <> target.size_bytes OR "
        "source.mtime_ns <> target.mtime_ns"
    )
    ctx = context.runtime.session_runtime().ctx
    dataset_location = context.runtime.profile.dataset_location("repo_snapshot")
    extra_constraints = resolve_delta_constraints(dataset_location) if dataset_location else ()
    from storage.deltalake import DeltaMergeArrowRequest

    delta_merge_arrow(
        ctx,
        request=DeltaMergeArrowRequest(
            path=str(target),
            source=snapshot,
            predicate="source.file_id = target.file_id",
            storage_options=storage.storage_options,
            log_storage_options=storage.log_storage_options,
            source_alias="source",
            target_alias="target",
            matched_predicate=update_predicate,
            update_all=True,
            insert_all=True,
            delete_not_matched_by_source=True,
            commit_properties=commit_properties,
            commit_metadata=commit_metadata,
            extra_constraints=extra_constraints,
            runtime_profile=context.runtime.profile,
            dataset_name="repo_snapshot",
        ),
    )
    context.runtime.profile.finalize_delta_commit(
        key=commit_key,
        run=commit_run,
        metadata={"operation": "merge", "rows_affected": snapshot.num_rows},
    )
    enable_delta_features(
        DeltaFeatureMutationOptions(
            path=str(target),
            storage_options=storage.storage_options,
            log_storage_options=storage.log_storage_options,
            runtime_profile=context.runtime.profile,
            dataset_name="repo_snapshot",
        )
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
