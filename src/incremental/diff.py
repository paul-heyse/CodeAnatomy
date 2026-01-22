"""Snapshot diff helpers for incremental pipeline runs."""

from __future__ import annotations

import contextlib
import uuid
from pathlib import Path

import pyarrow as pa
from datafusion import SessionContext
from deltalake import DeltaTable

from arrowdsl.schema.serialization import schema_fingerprint
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql_options import sql_options_for_profile
from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaCdfOptions,
    DeltaWriteOptions,
    DeltaWriteResult,
    enable_delta_features,
    write_table_delta,
)
from storage.deltalake.delta import read_delta_cdf


def _session_profile() -> DataFusionRuntimeProfile:
    return DataFusionRuntimeProfile()


def _session_context(profile: DataFusionRuntimeProfile) -> SessionContext:
    return profile.session_context()


def _register_table(ctx: SessionContext, table: pa.Table, *, prefix: str) -> str:
    name = f"__diff_{prefix}_{uuid.uuid4().hex}"
    ctx.register_record_batches(name, [list(table.to_batches())])
    return name


def _deregister_table(ctx: SessionContext, name: str | None) -> None:
    if name is None:
        return
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


def _added_sql(cur_table: str) -> str:
    return f"""
    SELECT
      cur.file_id AS file_id,
      cur.path AS path,
      'added' AS change_kind,
      CAST(NULL AS STRING) AS prev_path,
      cur.path AS cur_path,
      CAST(NULL AS STRING) AS prev_file_sha256,
      cur.file_sha256 AS cur_file_sha256,
      CAST(NULL AS BIGINT) AS prev_size_bytes,
      cur.size_bytes AS cur_size_bytes,
      CAST(NULL AS BIGINT) AS prev_mtime_ns,
      cur.mtime_ns AS cur_mtime_ns
    FROM {cur_table} AS cur
    """


def _diff_sql(cur_table: str, prev_table: str) -> str:
    return f"""
    SELECT
      COALESCE(cur.file_id, prev.file_id) AS file_id,
      COALESCE(cur.path, prev.path) AS path,
      CASE
        WHEN cur.path IS NULL THEN 'deleted'
        WHEN prev.path IS NULL THEN 'added'
        WHEN cur.file_sha256 = prev.file_sha256 AND cur.path <> prev.path THEN 'renamed'
        WHEN cur.file_sha256 <> prev.file_sha256 THEN 'modified'
        ELSE 'unchanged'
      END AS change_kind,
      prev.path AS prev_path,
      cur.path AS cur_path,
      prev.file_sha256 AS prev_file_sha256,
      cur.file_sha256 AS cur_file_sha256,
      prev.size_bytes AS prev_size_bytes,
      cur.size_bytes AS cur_size_bytes,
      prev.mtime_ns AS prev_mtime_ns,
      cur.mtime_ns AS cur_mtime_ns
    FROM {cur_table} AS cur
    FULL OUTER JOIN {prev_table} AS prev
      ON cur.file_id = prev.file_id
    """


def _diff_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table:
    """Diff two repo snapshots and return a change table.

    .. deprecated::
        This is a legacy function kept for internal use only.
        Use diff_snapshots_with_delta_cdf() for the primary CDF-driven path.

    Returns
    -------
    pa.Table
        Change records with per-file deltas.
    """
    profile = _session_profile()
    ctx = _session_context(profile)
    sql_options = profile.sql_options()
    cur_name = _register_table(ctx, cur, prefix="cur")
    prev_name: str | None = None
    try:
        if prev is None:
            return ctx.sql_with_options(_added_sql(cur_name), sql_options).to_arrow_table()
        prev_name = _register_table(ctx, prev, prefix="prev")
        return ctx.sql_with_options(_diff_sql(cur_name, prev_name), sql_options).to_arrow_table()
    finally:
        _deregister_table(ctx, cur_name)
        _deregister_table(ctx, prev_name)


def diff_snapshots_with_cdf(
    prev: pa.Table | None,
    cur: pa.Table,
    *,
    ctx: SessionContext,
    cdf_table: str,
) -> pa.Table:
    """Diff snapshots using CDF to limit the compared rows.

    Returns
    -------
    pa.Table
        Change records derived from the filtered snapshot diff.
    """
    cur_name = _register_table(ctx, cur, prefix="cur")
    prev_name: str | None = None
    sql_options = sql_options_for_profile(None)
    try:
        if prev is None:
            sql = (
                "WITH cur AS ("
                f"SELECT * FROM {cur_name} "
                f"WHERE file_id IN (SELECT DISTINCT file_id FROM {cdf_table})"
                ") " + _added_sql("cur")
            )
            return ctx.sql_with_options(sql, sql_options).to_arrow_table()
        prev_name = _register_table(ctx, prev, prefix="prev")
        sql = (
            "WITH cur AS ("
            f"SELECT * FROM {cur_name} "
            f"WHERE file_id IN (SELECT DISTINCT file_id FROM {cdf_table})"
            "), prev AS ("
            f"SELECT * FROM {prev_name} "
            f"WHERE file_id IN (SELECT DISTINCT file_id FROM {cdf_table})"
            ") " + _diff_sql("cur", "prev")
        )
        return ctx.sql_with_options(sql, sql_options).to_arrow_table()
    finally:
        _deregister_table(ctx, cur_name)
        _deregister_table(ctx, prev_name)


def diff_snapshots_with_delta_cdf(
    *,
    dataset_path: str,
    cursor_store: CdfCursorStore,
    dataset_name: str,
    filter_policy: CdfFilterPolicy | None = None,
) -> pa.Table | None:
    """Diff snapshots using Delta CDF for efficient incremental reads.

    This function reads changes from a Delta table's change data feed
    starting from the last processed version tracked by the cursor store.

    Parameters
    ----------
    dataset_path : str
        Path to the Delta table.
    cursor_store : CdfCursorStore
        Store managing CDF cursors.
    dataset_name : str
        Name of the dataset for cursor tracking.
    filter_policy : CdfFilterPolicy | None
        Policy for filtering change types. If None, includes all changes.

    Returns
    -------
    pa.Table | None
        CDF changes table, or None if CDF is not available.

    """
    # Check if Delta table exists
    if not Path(dataset_path).exists():
        return None

    # Check if it's a Delta table
    if not DeltaTable.is_deltatable(dataset_path):
        return None

    # Get current cursor
    cursor = cursor_store.load_cursor(dataset_name)

    # Open Delta table to get current version
    dt = DeltaTable(dataset_path)
    current_version = dt.version()

    # If no cursor exists, return None to signal full snapshot comparison
    if cursor is None:
        # Create initial cursor at current version
        cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))
        return None

    # If versions are the same, no changes
    if cursor.last_version >= current_version:
        return None

    # Read CDF changes
    starting_version = cursor.last_version + 1
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
    )

    try:
        cdf_table = read_delta_cdf(
            dataset_path,
            cdf_options=cdf_options,
        )
    except ValueError:
        # CDF not enabled - fall back to None to signal full comparison
        return None

    # Apply filter policy if provided
    if filter_policy is not None:
        predicate = filter_policy.to_sql_predicate()
        if predicate is not None and predicate != "FALSE":
            profile = _session_profile()
            ctx = _session_context(profile)
            sql_options = profile.sql_options()
            temp_name = f"__cdf_filter_{uuid.uuid4().hex}"
            ctx.register_record_batches(temp_name, [list(cdf_table.to_batches())])
            try:
                filtered_df = ctx.sql_with_options(
                    f"SELECT * FROM {temp_name} WHERE {predicate}",
                    sql_options,
                )
                cdf_table = filtered_df.to_arrow_table()
            finally:
                _deregister_table(ctx, temp_name)
        elif predicate == "FALSE":
            # No changes match filter
            cdf_table = pa.table({}, schema=cdf_table.schema)

    # Update cursor to current version
    cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))

    return cdf_table


def write_incremental_diff(store: StateStore, diff: pa.Table) -> DeltaWriteResult:
    """Persist the incremental diff to the state store as Delta.

    Returns
    -------
    str
        Path to the written diff Delta table.
    """
    store.ensure_dirs()
    target = store.incremental_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    result = write_table_delta(
        diff,
        str(target),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={
                "snapshot_kind": "incremental_diff",
                "schema_fingerprint": schema_fingerprint(diff.schema),
            },
        ),
    )
    enable_delta_features(result.path)
    return result


__all__ = [
    "diff_snapshots_with_cdf",
    "diff_snapshots_with_delta_cdf",
    "write_incremental_diff",
]
