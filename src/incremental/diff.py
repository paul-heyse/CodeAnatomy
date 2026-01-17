"""Snapshot diff helpers for incremental pipeline runs."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from arrowdsl.core.interop import pc
from arrowdsl.io.parquet import write_table_parquet
from arrowdsl.schema.build import table_from_arrays
from incremental.state_store import StateStore


def _coalesce_str(left: pa.Array, right: pa.Array) -> pa.Array:
    return cast("pa.Array", pc.if_else(pc.is_valid(left), left, right))


def _joined_columns(joined: pa.Table) -> dict[str, pa.Array]:
    return {
        "cur_path": cast("pa.Array", joined["path_cur"]),
        "prev_path": cast("pa.Array", joined["path_prev"]),
        "cur_sha": cast("pa.Array", joined["file_sha256_cur"]),
        "prev_sha": cast("pa.Array", joined["file_sha256_prev"]),
        "cur_size": cast("pa.Array", joined["size_bytes_cur"]),
        "prev_size": cast("pa.Array", joined["size_bytes_prev"]),
        "cur_mtime": cast("pa.Array", joined["mtime_ns_cur"]),
        "prev_mtime": cast("pa.Array", joined["mtime_ns_prev"]),
    }


def diff_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table:
    """Diff two repo snapshots and return a change table.

    Returns
    -------
    pa.Table
        Change records with per-file deltas.
    """
    schema = pa.schema(
        [
            pa.field("file_id", pa.string()),
            pa.field("path", pa.string()),
            pa.field("change_kind", pa.string()),
            pa.field("prev_path", pa.string()),
            pa.field("cur_path", pa.string()),
            pa.field("prev_file_sha256", pa.string()),
            pa.field("cur_file_sha256", pa.string()),
            pa.field("prev_size_bytes", pa.int64()),
            pa.field("cur_size_bytes", pa.int64()),
            pa.field("prev_mtime_ns", pa.int64()),
            pa.field("cur_mtime_ns", pa.int64()),
        ]
    )
    if prev is None:
        return table_from_arrays(
            schema,
            columns={
                "file_id": cur["file_id"],
                "path": cur["path"],
                "change_kind": pa.array(["added"] * cur.num_rows, type=pa.string()),
                "prev_path": pa.nulls(cur.num_rows, type=pa.string()),
                "cur_path": cur["path"],
                "prev_file_sha256": pa.nulls(cur.num_rows, type=pa.string()),
                "cur_file_sha256": cur["file_sha256"],
                "prev_size_bytes": pa.nulls(cur.num_rows, type=pa.int64()),
                "cur_size_bytes": cur["size_bytes"],
                "prev_mtime_ns": pa.nulls(cur.num_rows, type=pa.int64()),
                "cur_mtime_ns": cur["mtime_ns"],
            },
            num_rows=cur.num_rows,
        )

    joined = cur.join(
        prev,
        keys=["file_id"],
        join_type="full outer",
        left_suffix="_cur",
        right_suffix="_prev",
        coalesce_keys=True,
    )
    cols = _joined_columns(joined)
    missing_cur = pc.is_null(cols["cur_path"])
    missing_prev = pc.is_null(cols["prev_path"])
    renamed = pc.and_(
        pc.is_valid(cols["cur_path"]),
        pc.and_(
            pc.is_valid(cols["prev_path"]),
            pc.and_(
                pc.equal(cols["cur_sha"], cols["prev_sha"]),
                pc.not_equal(cols["cur_path"], cols["prev_path"]),
            ),
        ),
    )
    modified = pc.and_(
        pc.is_valid(cols["cur_path"]),
        pc.and_(
            pc.is_valid(cols["prev_path"]),
            pc.not_equal(cols["cur_sha"], cols["prev_sha"]),
        ),
    )
    change_kind = pc.case_when(
        [
            (missing_cur, "deleted"),
            (missing_prev, "added"),
            (renamed, "renamed"),
            (modified, "modified"),
        ],
        "unchanged",
    )
    path = _coalesce_str(cols["cur_path"], cols["prev_path"])
    return table_from_arrays(
        schema,
        columns={
            "file_id": joined["file_id"],
            "path": path,
            "change_kind": cast("pa.Array", change_kind),
            "prev_path": cols["prev_path"],
            "cur_path": cols["cur_path"],
            "prev_file_sha256": cols["prev_sha"],
            "cur_file_sha256": cols["cur_sha"],
            "prev_size_bytes": cols["prev_size"],
            "cur_size_bytes": cols["cur_size"],
            "prev_mtime_ns": cols["prev_mtime"],
            "cur_mtime_ns": cols["cur_mtime"],
        },
        num_rows=joined.num_rows,
    )


def write_incremental_diff(store: StateStore, diff: pa.Table) -> str:
    """Persist the incremental diff to the state store.

    Returns
    -------
    str
        Path to the written diff parquet file.
    """
    store.ensure_dirs()
    target = store.incremental_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    return write_table_parquet(diff, target, overwrite=True)


__all__ = ["diff_snapshots", "write_incremental_diff"]
