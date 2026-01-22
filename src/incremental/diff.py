"""Snapshot diff helpers for incremental pipeline runs."""

from __future__ import annotations

import contextlib
import uuid

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.schema.serialization import schema_fingerprint
from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    enable_delta_features,
    write_table_delta,
)


def _session_context() -> SessionContext:
    return DataFusionRuntimeProfile().session_context()


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


def diff_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table:
    """Diff two repo snapshots and return a change table.

    Returns
    -------
    pa.Table
        Change records with per-file deltas.
    """
    ctx = _session_context()
    cur_name = _register_table(ctx, cur, prefix="cur")
    prev_name: str | None = None
    try:
        if prev is None:
            return ctx.sql(_added_sql(cur_name)).to_arrow_table()
        prev_name = _register_table(ctx, prev, prefix="prev")
        return ctx.sql(_diff_sql(cur_name, prev_name)).to_arrow_table()
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
    try:
        if prev is None:
            sql = (
                "WITH cur AS ("
                f"SELECT * FROM {cur_name} "
                f"WHERE file_id IN (SELECT DISTINCT file_id FROM {cdf_table})"
                ") " + _added_sql("cur")
            )
            return ctx.sql(sql).to_arrow_table()
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
        return ctx.sql(sql).to_arrow_table()
    finally:
        _deregister_table(ctx, cur_name)
        _deregister_table(ctx, prev_name)


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


__all__ = ["diff_snapshots", "diff_snapshots_with_cdf", "write_incremental_diff"]
