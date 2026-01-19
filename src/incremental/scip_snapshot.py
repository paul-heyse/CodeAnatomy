"""SCIP snapshot and diff helpers for incremental pipeline runs."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from typing import cast

import pyarrow as pa

from arrowdsl.core.ids import hash64_from_arrays
from arrowdsl.core.interop import TableLike, pc
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

_HASH_NULL_SENTINEL = "None"

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


def _as_py_value(value: object) -> object:
    as_py = getattr(value, "as_py", None)
    return as_py() if callable(as_py) else value


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
    docs = cast("pa.Table", scip_documents)
    doc_paths, doc_fingerprint_inputs = _document_inputs(docs)
    occ_counts, diag_counts = _occurrence_and_diagnostic_counts(
        cast("pa.Table", scip_occurrences),
        cast("pa.Table", scip_diagnostics),
        doc_fingerprint_inputs,
    )
    doc_id_list = sorted(doc_paths)
    schema = _snapshot_schema()
    return table_from_arrays(
        schema,
        columns={
            "document_id": pa.array(doc_id_list, type=pa.string()),
            "path": pa.array([doc_paths[doc_id] for doc_id in doc_id_list], type=pa.string()),
            "fingerprint": pa.array(
                [_fingerprint(doc_fingerprint_inputs.get(doc_id, [])) for doc_id in doc_id_list],
                type=pa.string(),
            ),
            "occurrence_count": pa.array(
                [occ_counts.get(doc_id, 0) for doc_id in doc_id_list], type=pa.int64()
            ),
            "diagnostic_count": pa.array(
                [diag_counts.get(doc_id, 0) for doc_id in doc_id_list], type=pa.int64()
            ),
        },
        num_rows=len(doc_id_list),
    )


def _document_inputs(
    docs: pa.Table,
) -> tuple[dict[str, str | None], dict[str, list[int]]]:
    doc_ids = column_or_null(docs, "document_id", pa.string())
    paths = column_or_null(docs, "path", pa.string())
    doc_hashes = _row_hashes(docs, columns=_DOC_HASH_COLUMNS, prefix="scip_doc")
    doc_paths: dict[str, str | None] = {}
    doc_fingerprint_inputs: dict[str, list[int]] = {}
    _accumulate_hashes(
        doc_ids,
        doc_hashes,
        doc_fingerprint_inputs,
        counts=None,
    )
    for doc_id_value, path_value in zip(doc_ids, paths, strict=False):
        doc_id = _as_py_value(doc_id_value)
        path = _as_py_value(path_value)
        if not isinstance(doc_id, str) or not doc_id:
            continue
        if doc_id in doc_paths:
            continue
        doc_paths[doc_id] = path if isinstance(path, str) else None
    return doc_paths, doc_fingerprint_inputs


def _occurrence_and_diagnostic_counts(
    occs: pa.Table,
    diags: pa.Table,
    doc_fingerprint_inputs: dict[str, list[int]],
) -> tuple[dict[str, int], dict[str, int]]:
    occ_counts: dict[str, int] = {}
    diag_counts: dict[str, int] = {}
    if occs.num_rows:
        occ_hashes = _row_hashes(occs, columns=_OCCURRENCE_HASH_COLUMNS, prefix="scip_occ")
        _accumulate_hashes(
            column_or_null(occs, "document_id", pa.string()),
            occ_hashes,
            doc_fingerprint_inputs,
            counts=occ_counts,
        )
    if diags.num_rows:
        diag_hashes = _row_hashes(diags, columns=_DIAGNOSTIC_HASH_COLUMNS, prefix="scip_diag")
        _accumulate_hashes(
            column_or_null(diags, "document_id", pa.string()),
            diag_hashes,
            doc_fingerprint_inputs,
            counts=diag_counts,
        )
    return occ_counts, diag_counts


def _snapshot_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("document_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("fingerprint", pa.string()),
            pa.field("occurrence_count", pa.int64()),
            pa.field("diagnostic_count", pa.int64()),
        ]
    )


def diff_scip_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table:
    """Diff two SCIP snapshots and return a change table.

    Returns
    -------
    pa.Table
        Change records with per-document deltas.
    """
    schema = pa.schema(
        [
            pa.field("document_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("change_kind", pa.string()),
            pa.field("prev_path", pa.string()),
            pa.field("cur_path", pa.string()),
            pa.field("prev_fingerprint", pa.string()),
            pa.field("cur_fingerprint", pa.string()),
        ]
    )
    if prev is None:
        return table_from_arrays(
            schema,
            columns={
                "document_id": cur["document_id"],
                "path": cur["path"],
                "change_kind": pa.array(["added"] * cur.num_rows, type=pa.string()),
                "prev_path": pa.nulls(cur.num_rows, type=pa.string()),
                "cur_path": cur["path"],
                "prev_fingerprint": pa.nulls(cur.num_rows, type=pa.string()),
                "cur_fingerprint": cur["fingerprint"],
            },
            num_rows=cur.num_rows,
        )

    joined = cur.join(
        prev,
        keys=["document_id"],
        join_type="full outer",
        left_suffix="_cur",
        right_suffix="_prev",
        coalesce_keys=True,
    )
    cur_path = cast("pa.Array", joined["path_cur"])
    prev_path = cast("pa.Array", joined["path_prev"])
    cur_fingerprint = cast("pa.Array", joined["fingerprint_cur"])
    prev_fingerprint = cast("pa.Array", joined["fingerprint_prev"])
    missing_cur = pc.is_null(cur_path)
    missing_prev = pc.is_null(prev_path)
    modified = pc.and_(
        pc.is_valid(cur_path),
        pc.and_(
            pc.is_valid(prev_path),
            pc.not_equal(cur_fingerprint, prev_fingerprint),
        ),
    )
    change_kind = pc.case_when(
        [
            (missing_cur, "deleted"),
            (missing_prev, "added"),
            (modified, "modified"),
        ],
        "unchanged",
    )
    path = _coalesce_str(cur_path, prev_path)
    return table_from_arrays(
        schema,
        columns={
            "document_id": joined["document_id"],
            "path": path,
            "change_kind": cast("pa.Array", change_kind),
            "prev_path": prev_path,
            "cur_path": cur_path,
            "prev_fingerprint": prev_fingerprint,
            "cur_fingerprint": cur_fingerprint,
        },
        num_rows=joined.num_rows,
    )


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
    changed_paths = _changed_paths(diff)
    if not changed_paths:
        return ()
    path_to_file_id = _path_to_file_id(repo_snapshot)
    file_ids = [path_to_file_id.get(path) for path in changed_paths]
    cleaned = [value for value in file_ids if isinstance(value, str) and value]
    return tuple(sorted(set(cleaned)))


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
    return cast("pa.Table", read_table_delta(str(path)))


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


def _row_hashes(
    table: pa.Table,
    *,
    columns: Sequence[tuple[str, pa.DataType]],
    prefix: str,
) -> pa.Array:
    arrays = [column_or_null(table, name, dtype) for name, dtype in columns]
    return cast(
        "pa.Array",
        hash64_from_arrays(arrays, prefix=prefix, null_sentinel=_HASH_NULL_SENTINEL),
    )


def _accumulate_hashes(
    doc_ids: pa.Array,
    hashes: pa.Array,
    out: dict[str, list[int]],
    *,
    counts: dict[str, int] | None,
) -> None:
    for doc_id_value, hash_value in zip(doc_ids, hashes, strict=False):
        doc_id = _as_py_value(doc_id_value)
        value = _as_py_value(hash_value)
        if not isinstance(doc_id, str) or not doc_id:
            continue
        if value is None:
            continue
        if isinstance(value, int) and not isinstance(value, bool):
            value_int = value
        elif isinstance(value, (str, bytes, bytearray, float)):
            try:
                value_int = int(value)
            except (TypeError, ValueError):
                continue
        else:
            continue
        out.setdefault(doc_id, []).append(value_int)
        if counts is not None:
            counts[doc_id] = counts.get(doc_id, 0) + 1


def _fingerprint(values: Iterable[int]) -> str:
    hasher = hashlib.sha256()
    for item in sorted(values):
        hasher.update(str(item).encode("utf-8"))
        hasher.update(b"\0")
    return hasher.hexdigest()


def _changed_paths(diff: pa.Table) -> list[str]:
    if diff.num_rows == 0:
        return []
    change_kind = diff["change_kind"]
    paths = diff["path"]
    mask = pc.not_equal(change_kind, pa.scalar("unchanged", type=pa.string()))
    filtered = pc.filter(paths, mask)
    values = [_as_py_value(value) for value in filtered]
    return [value for value in values if isinstance(value, str) and value]


def _path_to_file_id(snapshot: pa.Table) -> Mapping[str, str]:
    mapping: dict[str, str] = {}
    for path_value, file_id_value in zip(
        snapshot["path"],
        snapshot["file_id"],
        strict=False,
    ):
        path = _as_py_value(path_value)
        file_id = _as_py_value(file_id_value)
        if isinstance(path, str) and path and isinstance(file_id, str) and file_id:
            mapping.setdefault(path, file_id)
    return mapping


def _coalesce_str(left: pa.Array, right: pa.Array) -> pa.Array:
    return cast("pa.Array", pc.if_else(pc.is_valid(left), left, right))


__all__ = [
    "build_scip_snapshot",
    "diff_scip_snapshots",
    "read_scip_snapshot",
    "scip_changed_file_ids",
    "write_scip_diff",
    "write_scip_snapshot",
]
