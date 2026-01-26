"""SCIP snapshot and diff helpers for incremental pipeline runs."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa

from arrowdsl.core.interop import coerce_table_like
from arrowdsl.schema.abi import schema_fingerprint
from ibis_engine.builtin_udfs import ibis_udf_call
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from incremental.delta_context import read_delta_table_via_facade
from incremental.ibis_exec import ibis_expr_to_table
from incremental.ibis_utils import ibis_table_from_arrow
from storage.deltalake import delta_table_version, enable_delta_features

if TYPE_CHECKING:
    from ibis.expr.types import StringValue, Value

    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
    from incremental.delta_context import DeltaAccessContext
    from incremental.runtime import IncrementalRuntime
    from incremental.state_store import StateStore
    from storage.deltalake import DeltaWriteResult

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


def _concat_with_sep(parts: Sequence[StringValue], sep: str) -> StringValue:
    if not parts:
        msg = "Expected at least one string part."
        raise ValueError(msg)
    result = parts[0]
    for part in parts[1:]:
        result = result.concat(sep, part)
    return result


def _row_hash_expr(
    prefix: str,
    table: ibis.Table,
    columns: Sequence[tuple[str, pa.DataType]],
) -> Value:
    parts: list[StringValue] = [ibis.literal(prefix).cast("string")]
    for name, _dtype in columns:
        value = table[name].cast("string")
        parts.append(ibis.coalesce(value, ibis.literal(_HASH_NULL_SENTINEL)).cast("string"))
    return ibis_udf_call("stable_hash64", _concat_with_sep(parts, _HASH_SEPARATOR))


def _as_table(table: TableLike) -> pa.Table:
    resolved = coerce_table_like(table)
    if isinstance(resolved, pa.RecordBatchReader):
        reader = cast("RecordBatchReaderLike", resolved)
        return pa.Table.from_batches(list(reader))
    return cast("pa.Table", resolved)


def _filter_document_id(table: ibis.Table) -> ibis.Table:
    doc_id = cast("StringValue", table.document_id)
    return table.filter(ibis.and_(doc_id.notnull(), doc_id != ibis.literal("")))


def _hash_rows(
    table: ibis.Table,
    *,
    prefix: str,
    columns: Sequence[tuple[str, pa.DataType]],
    include_path: bool,
) -> ibis.Table:
    row_hash = _row_hash_expr(prefix, table, columns).cast("string")
    result = table.select(
        document_id=table.document_id,
        row_hash=row_hash,
    )
    if include_path:
        result = result.mutate(path=table.path)
    return result


def _count_by_document(table: ibis.Table, *, column_name: str) -> ibis.Table:
    return table.group_by("document_id").aggregate(**{column_name: table.document_id.count()})


def build_scip_snapshot(
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    scip_diagnostics: TableLike,
    *,
    runtime: IncrementalRuntime,
) -> pa.Table:
    """Return a document-level SCIP snapshot table.

    Parameters
    ----------
    scip_documents:
        SCIP document table.
    scip_occurrences:
        SCIP occurrence table.
    scip_diagnostics:
        SCIP diagnostic table.
    runtime : IncrementalRuntime
        Shared incremental runtime for DataFusion execution.

    Returns
    -------
    pa.Table
        Snapshot table keyed by document_id with fingerprints and counts.
    """
    backend = runtime.ibis_backend()
    docs = _filter_document_id(ibis_table_from_arrow(backend, _as_table(scip_documents)))
    occs = _filter_document_id(ibis_table_from_arrow(backend, _as_table(scip_occurrences)))
    diags = _filter_document_id(ibis_table_from_arrow(backend, _as_table(scip_diagnostics)))

    docs_hashes = _hash_rows(
        docs,
        prefix="scip_doc",
        columns=_DOC_HASH_COLUMNS,
        include_path=True,
    )
    occ_hashes = _hash_rows(
        occs,
        prefix="scip_occ",
        columns=_OCCURRENCE_HASH_COLUMNS,
        include_path=False,
    )
    diag_hashes = _hash_rows(
        diags,
        prefix="scip_diag",
        columns=_DIAGNOSTIC_HASH_COLUMNS,
        include_path=False,
    )
    hashes = ibis.union(
        docs_hashes.select("document_id", "row_hash"),
        occ_hashes.select("document_id", "row_hash"),
        diag_hashes.select("document_id", "row_hash"),
        distinct=False,
    ).distinct()

    fingerprints = hashes.group_by("document_id").aggregate(
        fingerprint=ibis_udf_call(
            "sha256",
            hashes.row_hash.group_concat(
                sep=_HASH_SEPARATOR,
                order_by=hashes.row_hash,
            ),
        )
    )
    occ_counts = _count_by_document(occs, column_name="occurrence_count")
    diag_counts = _count_by_document(diags, column_name="diagnostic_count")
    joined = (
        docs.left_join(fingerprints, ["document_id"])
        .left_join(occ_counts, ["document_id"])
        .left_join(diag_counts, ["document_id"])
    )
    result = joined.select(
        document_id=joined.document_id,
        path=joined.path,
        fingerprint=joined.fingerprint,
        occurrence_count=ibis.coalesce(joined.occurrence_count, ibis.literal(0)),
        diagnostic_count=ibis.coalesce(joined.diagnostic_count, ibis.literal(0)),
    )
    return ibis_expr_to_table(result, runtime=runtime, name="scip_snapshot")


def diff_scip_snapshots(
    prev: pa.Table | None,
    cur: pa.Table,
    *,
    runtime: IncrementalRuntime,
) -> pa.Table:
    """Diff two SCIP snapshots and return a change table.

    Parameters
    ----------
    prev:
        Previous snapshot table, if available.
    cur:
        Current snapshot table.
    runtime : IncrementalRuntime
        Shared incremental runtime for DataFusion execution.

    Returns
    -------
    pa.Table
        Change records with per-document deltas.
    """
    backend = runtime.ibis_backend()
    cur_table = _as_table(cur)
    cur_expr = ibis_table_from_arrow(backend, cur_table)
    if prev is None:
        result = cur_expr.select(
            document_id=cur_expr.document_id,
            path=cur_expr.path,
            change_kind=ibis.literal("added"),
            prev_path=ibis.null("string"),
            cur_path=cur_expr.path,
            prev_fingerprint=ibis.null("string"),
            cur_fingerprint=cur_expr.fingerprint,
        )
        return ibis_expr_to_table(result, runtime=runtime, name="scip_diff_added")
    prev_table = _as_table(prev)
    prev_expr = ibis_table_from_arrow(backend, prev_table)
    joined = cur_expr.outer_join(
        prev_expr,
        predicates=[cur_expr.document_id == prev_expr.document_id],
        rname="{name}_prev",
    )
    change_kind = ibis.ifelse(
        joined.path.isnull(),
        "deleted",
        ibis.ifelse(
            joined.path_prev.isnull(),
            "added",
            ibis.ifelse(
                joined.fingerprint != joined.fingerprint_prev,
                "modified",
                "unchanged",
            ),
        ),
    )
    result = joined.select(
        document_id=ibis.coalesce(joined.document_id, joined.document_id_prev),
        path=ibis.coalesce(joined.path, joined.path_prev),
        change_kind=change_kind,
        prev_path=joined.path_prev,
        cur_path=joined.path,
        prev_fingerprint=joined.fingerprint_prev,
        cur_fingerprint=joined.fingerprint,
    )
    return ibis_expr_to_table(result, runtime=runtime, name="scip_diff")


def scip_changed_file_ids(
    diff: pa.Table | None,
    repo_snapshot: pa.Table | None,
    *,
    runtime: IncrementalRuntime,
) -> tuple[str, ...]:
    """Return repo file_ids impacted by SCIP document changes.

    Parameters
    ----------
    diff:
        Snapshot diff table.
    repo_snapshot:
        Repo snapshot table mapping paths to file ids.
    runtime : IncrementalRuntime
        Shared incremental runtime for DataFusion execution.

    Returns
    -------
    tuple[str, ...]
        Sorted file_id values corresponding to changed SCIP documents.
    """
    if diff is None or repo_snapshot is None or diff.num_rows == 0:
        return ()
    backend = runtime.ibis_backend()
    diff_expr = ibis_table_from_arrow(backend, diff)
    repo_expr = ibis_table_from_arrow(backend, repo_snapshot)
    path_expr = cast("StringValue", diff_expr.path)
    changed_filter = ibis.and_(
        diff_expr.change_kind != ibis.literal("unchanged"),
        path_expr.notnull(),
        path_expr != ibis.literal(""),
    )
    changed = diff_expr.filter(changed_filter)
    changed_paths = changed.select(path=changed.path).distinct()
    joined = repo_expr.inner_join(
        changed_paths,
        predicates=[repo_expr.path == changed_paths.path],
    )
    result = joined.select(file_id=repo_expr.file_id).distinct()
    table = ibis_expr_to_table(result, runtime=runtime, name="scip_changed_file_ids")
    values = [value for value in table["file_id"].to_pylist() if isinstance(value, str) and value]
    return tuple(sorted(set(values)))


def read_scip_snapshot(
    store: StateStore,
    *,
    context: DeltaAccessContext,
) -> pa.Table | None:
    """Load the previous SCIP snapshot when present.

    Returns
    -------
    pa.Table | None
        Loaded snapshot table, or ``None`` when missing.
    """
    path = store.scip_snapshot_path()
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
    return _read_delta_table(context, path, name="scip_snapshot_read")


def write_scip_snapshot(
    store: StateStore,
    snapshot: pa.Table,
    *,
    context: DeltaAccessContext,
) -> DeltaWriteResult:
    """Persist the SCIP snapshot to the state store as Delta.

    Returns
    -------
    str
        Path to the persisted snapshot Delta table.
    """
    store.ensure_dirs()
    target = store.scip_snapshot_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    commit_metadata = {
        "snapshot_kind": "scip_snapshot",
        "schema_fingerprint": schema_fingerprint(snapshot.schema),
    }
    result = write_ibis_dataset_delta(
        snapshot,
        str(target),
        options=IbisDatasetWriteOptions(
            execution=context.runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=commit_metadata,
                storage_options=context.storage.storage_options,
                log_storage_options=context.storage.log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=context.storage.storage_options,
        log_storage_options=context.storage.log_storage_options,
    )
    return result


def write_scip_diff(
    store: StateStore,
    diff: pa.Table,
    *,
    context: DeltaAccessContext,
) -> DeltaWriteResult:
    """Persist the SCIP diff to the state store as Delta.

    Returns
    -------
    str
        Path to the persisted diff Delta table.
    """
    store.ensure_dirs()
    target = store.scip_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    commit_metadata = {
        "snapshot_kind": "scip_diff",
        "schema_fingerprint": schema_fingerprint(diff.schema),
    }
    result = write_ibis_dataset_delta(
        diff,
        str(target),
        options=IbisDatasetWriteOptions(
            execution=context.runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=commit_metadata,
                storage_options=context.storage.storage_options,
                log_storage_options=context.storage.log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=context.storage.storage_options,
        log_storage_options=context.storage.log_storage_options,
    )
    return result


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
    *,
    name: str,
) -> pa.Table:
    return read_delta_table_via_facade(context, path=path, name=name)


__all__ = [
    "build_scip_snapshot",
    "diff_scip_snapshots",
    "read_scip_snapshot",
    "scip_changed_file_ids",
    "write_scip_diff",
    "write_scip_snapshot",
]
