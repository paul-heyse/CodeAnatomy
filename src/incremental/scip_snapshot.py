"""SCIP snapshot and diff helpers for incremental pipeline runs."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Sequence
from typing import cast

import pyarrow as pa
from datafusion import SessionContext

from arrowdsl.core.interop import RecordBatchReaderLike, Scalar, TableLike, coerce_table_like
from arrowdsl.schema.serialization import schema_fingerprint
from datafusion_engine.runtime import DataFusionRuntimeProfile, read_delta_as_reader
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    delta_table_version,
    enable_delta_features,
    write_table_delta,
)

_HASH_NULL_SENTINEL = "None"
_HASH_SEPARATOR = "\x1f"

_DOC_HASH_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("document_id", pa.string()),
    ("path", pa.string()),
    ("language", pa.string()),
    ("position_encoding", pa.string()),
)
_OCCURRENCE_HASH_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("document_id", pa.string()),
    ("path", pa.string()),
    ("symbol", pa.string()),
    ("symbol_roles", pa.int32()),
    ("syntax_kind", pa.string()),
    ("start_line", pa.int32()),
    ("start_char", pa.int32()),
    ("end_line", pa.int32()),
    ("end_char", pa.int32()),
    ("range_len", pa.int32()),
    ("enc_start_line", pa.int32()),
    ("enc_start_char", pa.int32()),
    ("enc_end_line", pa.int32()),
    ("enc_end_char", pa.int32()),
    ("enc_range_len", pa.int32()),
    ("line_base", pa.int32()),
    ("col_unit", pa.string()),
    ("end_exclusive", pa.bool_()),
)
_DIAGNOSTIC_HASH_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("document_id", pa.string()),
    ("path", pa.string()),
    ("severity", pa.string()),
    ("code", pa.string()),
    ("message", pa.string()),
    ("source", pa.string()),
    ("tags", pa.large_list(pa.string())),
    ("start_line", pa.int32()),
    ("start_char", pa.int32()),
    ("end_line", pa.int32()),
    ("end_char", pa.int32()),
    ("line_base", pa.int32()),
    ("col_unit", pa.string()),
    ("end_exclusive", pa.bool_()),
)


def _session_profile() -> DataFusionRuntimeProfile:
    return DataFusionRuntimeProfile()


def _session_context(profile: DataFusionRuntimeProfile) -> SessionContext:
    return profile.session_context()


def _register_table(ctx: SessionContext, table: pa.Table, *, prefix: str) -> str:
    name = f"__scip_snapshot_{prefix}_{uuid.uuid4().hex}"
    ctx.register_record_batches(name, table.to_batches())
    return name


def _deregister_table(ctx: SessionContext, name: str | None) -> None:
    if name is None:
        return
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _stringify_expr(column: str) -> str:
    col_name = _sql_identifier(column)
    return f"COALESCE(CAST({col_name} AS STRING), '{_HASH_NULL_SENTINEL}')"


def _row_hash_expr(prefix: str, columns: Sequence[tuple[str, pa.DataType]]) -> str:
    parts = [f"'{prefix}'"]
    parts.extend(_stringify_expr(name) for name, _dtype in columns)
    joined = ", ".join(parts)
    return f"stable_hash64(concat_ws('{_HASH_SEPARATOR}', {joined}))"


def _as_table(table: TableLike) -> pa.Table:
    resolved = coerce_table_like(table)
    if isinstance(resolved, pa.RecordBatchReader):
        reader = cast("RecordBatchReaderLike", resolved)
        return pa.Table.from_batches(list(reader))
    return cast("pa.Table", resolved)


def _as_py_value(value: object) -> object:
    if isinstance(value, Scalar):
        return value.as_py()
    as_py = getattr(value, "as_py", None)
    if callable(as_py):
        return as_py()
    return value


def build_scip_snapshot(
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    scip_diagnostics: TableLike,
) -> pa.Table:
    """Return a document-level SCIP snapshot table.

    Returns
    -------
    pa.Table
        Snapshot table keyed by document_id with fingerprints and counts.
    """
    profile = _session_profile()
    ctx = _session_context(profile)
    sql_options = profile.sql_options()
    docs_table = _as_table(scip_documents)
    occs_table = _as_table(scip_occurrences)
    diags_table = _as_table(scip_diagnostics)
    docs_name = _register_table(ctx, docs_table, prefix="docs")
    occs_name = _register_table(ctx, occs_table, prefix="occ")
    diags_name = _register_table(ctx, diags_table, prefix="diag")
    try:
        doc_hash = _row_hash_expr("scip_doc", _DOC_HASH_COLUMNS)
        occ_hash = _row_hash_expr("scip_occ", _OCCURRENCE_HASH_COLUMNS)
        diag_hash = _row_hash_expr("scip_diag", _DIAGNOSTIC_HASH_COLUMNS)
        sql = f"""
        WITH docs AS (
          SELECT
            document_id,
            path,
            {doc_hash} AS row_hash
          FROM {docs_name}
          WHERE document_id IS NOT NULL AND document_id <> ''
        ),
        occ AS (
          SELECT
            document_id,
            {occ_hash} AS row_hash
          FROM {occs_name}
          WHERE document_id IS NOT NULL AND document_id <> ''
        ),
        diag AS (
          SELECT
            document_id,
            {diag_hash} AS row_hash
          FROM {diags_name}
          WHERE document_id IS NOT NULL AND document_id <> ''
        ),
        hashes AS (
          SELECT document_id, CAST(row_hash AS STRING) AS row_hash FROM docs
          UNION ALL
          SELECT document_id, CAST(row_hash AS STRING) AS row_hash FROM occ
          UNION ALL
          SELECT document_id, CAST(row_hash AS STRING) AS row_hash FROM diag
        ),
        fingerprints AS (
          SELECT
            document_id,
            sha256(
              array_to_string(
                array_sort(array_distinct(array_agg(row_hash))),
                '{_HASH_SEPARATOR}'
              )
            ) AS fingerprint
          FROM hashes
          GROUP BY document_id
        ),
        occ_counts AS (
          SELECT document_id, COUNT(*) AS occurrence_count
          FROM {occs_name}
          WHERE document_id IS NOT NULL AND document_id <> ''
          GROUP BY document_id
        ),
        diag_counts AS (
          SELECT document_id, COUNT(*) AS diagnostic_count
          FROM {diags_name}
          WHERE document_id IS NOT NULL AND document_id <> ''
          GROUP BY document_id
        )
        SELECT
          docs.document_id AS document_id,
          docs.path AS path,
          fingerprints.fingerprint AS fingerprint,
          COALESCE(occ_counts.occurrence_count, 0) AS occurrence_count,
          COALESCE(diag_counts.diagnostic_count, 0) AS diagnostic_count
        FROM docs
        LEFT JOIN fingerprints ON docs.document_id = fingerprints.document_id
        LEFT JOIN occ_counts ON docs.document_id = occ_counts.document_id
        LEFT JOIN diag_counts ON docs.document_id = diag_counts.document_id
        """
        return ctx.sql_with_options(sql, sql_options).to_arrow_table()
    finally:
        _deregister_table(ctx, docs_name)
        _deregister_table(ctx, occs_name)
        _deregister_table(ctx, diags_name)


def _snapshot_added_sql(cur_table: str) -> str:
    return f"""
    SELECT
      cur.document_id AS document_id,
      cur.path AS path,
      'added' AS change_kind,
      CAST(NULL AS STRING) AS prev_path,
      cur.path AS cur_path,
      CAST(NULL AS STRING) AS prev_fingerprint,
      cur.fingerprint AS cur_fingerprint
    FROM {cur_table} AS cur
    """


def _snapshot_diff_sql(cur_table: str, prev_table: str) -> str:
    return f"""
    SELECT
      COALESCE(cur.document_id, prev.document_id) AS document_id,
      COALESCE(cur.path, prev.path) AS path,
      CASE
        WHEN cur.path IS NULL THEN 'deleted'
        WHEN prev.path IS NULL THEN 'added'
        WHEN cur.fingerprint <> prev.fingerprint THEN 'modified'
        ELSE 'unchanged'
      END AS change_kind,
      prev.path AS prev_path,
      cur.path AS cur_path,
      prev.fingerprint AS prev_fingerprint,
      cur.fingerprint AS cur_fingerprint
    FROM {cur_table} AS cur
    FULL OUTER JOIN {prev_table} AS prev
      ON cur.document_id = prev.document_id
    """


def diff_scip_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table:
    """Diff two SCIP snapshots and return a change table.

    Returns
    -------
    pa.Table
        Change records with per-document deltas.
    """
    profile = _session_profile()
    ctx = _session_context(profile)
    sql_options = profile.sql_options()
    cur_table = _as_table(cur)
    cur_name = _register_table(ctx, cur_table, prefix="snapshot_cur")
    prev_name: str | None = None
    try:
        if prev is None:
            return ctx.sql_with_options(_snapshot_added_sql(cur_name), sql_options).to_arrow_table()
        prev_table = _as_table(prev)
        prev_name = _register_table(ctx, prev_table, prefix="snapshot_prev")
        return ctx.sql_with_options(
            _snapshot_diff_sql(cur_name, prev_name),
            sql_options,
        ).to_arrow_table()
    finally:
        _deregister_table(ctx, cur_name)
        _deregister_table(ctx, prev_name)


def scip_changed_file_ids(
    diff: pa.Table | None,
    repo_snapshot: pa.Table | None,
) -> tuple[str, ...]:
    """Return repo file_ids impacted by SCIP document changes.

    Returns
    -------
    tuple[str, ...]
        Sorted file_id values corresponding to changed SCIP documents.
    """
    if diff is None or repo_snapshot is None or diff.num_rows == 0:
        return ()
    profile = _session_profile()
    ctx = _session_context(profile)
    sql_options = profile.sql_options()
    diff_name = _register_table(ctx, diff, prefix="diff")
    repo_name = _register_table(ctx, repo_snapshot, prefix="repo")
    try:
        sql = f"""
        WITH changed AS (
          SELECT DISTINCT path
          FROM {diff_name}
          WHERE change_kind <> 'unchanged'
            AND path IS NOT NULL
            AND path <> ''
        )
        SELECT DISTINCT repo.file_id AS file_id
        FROM {repo_name} AS repo
        JOIN changed ON repo.path = changed.path
        """
        table = ctx.sql_with_options(sql, sql_options).to_arrow_table()
        values = [
            value for value in table["file_id"].to_pylist() if isinstance(value, str) and value
        ]
        return tuple(sorted(set(values)))
    finally:
        _deregister_table(ctx, diff_name)
        _deregister_table(ctx, repo_name)


def read_scip_snapshot(store: StateStore) -> pa.Table | None:
    """Load the previous SCIP snapshot when present.

    Returns
    -------
    pa.Table | None
        Loaded snapshot table, or ``None`` when missing.
    """
    path = store.scip_snapshot_path()
    if not path.exists() or delta_table_version(str(path)) is None:
        return None
    reader = read_delta_as_reader(str(path))
    return reader.read_all()


def write_scip_snapshot(store: StateStore, snapshot: pa.Table) -> DeltaWriteResult:
    """Persist the SCIP snapshot to the state store as Delta.

    Returns
    -------
    str
        Path to the persisted snapshot Delta table.
    """
    store.ensure_dirs()
    target = store.scip_snapshot_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    result = write_table_delta(
        snapshot,
        str(target),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={
                "snapshot_kind": "scip_snapshot",
                "schema_fingerprint": schema_fingerprint(snapshot.schema),
            },
        ),
    )
    enable_delta_features(result.path)
    return result


def write_scip_diff(store: StateStore, diff: pa.Table) -> DeltaWriteResult:
    """Persist the SCIP diff to the state store as Delta.

    Returns
    -------
    str
        Path to the persisted diff Delta table.
    """
    store.ensure_dirs()
    target = store.scip_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    result = write_table_delta(
        diff,
        str(target),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={
                "snapshot_kind": "scip_diff",
                "schema_fingerprint": schema_fingerprint(diff.schema),
            },
        ),
    )
    enable_delta_features(result.path)
    return result


__all__ = [
    "build_scip_snapshot",
    "diff_scip_snapshots",
    "read_scip_snapshot",
    "scip_changed_file_ids",
    "write_scip_diff",
    "write_scip_snapshot",
]
