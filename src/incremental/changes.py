"""Incremental change derivation helpers."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.interop import pc
from incremental.types import IncrementalFileChanges

_CHANGED_KINDS: tuple[str, ...] = ("added", "modified", "renamed")
_DELETED_KINDS: tuple[str, ...] = ("deleted",)
_CHANGED_KIND_SET = pa.array(list(_CHANGED_KINDS), type=pa.string())
_DELETED_KIND_SET = pa.array(list(_DELETED_KINDS), type=pa.string())


def file_changes_from_diff(diff: pa.Table | None) -> IncrementalFileChanges:
    """Derive file change sets from the incremental diff table.

    Parameters
    ----------
    diff:
        Diff table produced by ``diff_snapshots``.

    Returns
    -------
    IncrementalFileChanges
        Change sets grouped by changed vs deleted file ids.
    """
    if diff is None:
        return IncrementalFileChanges()
    change_kind = diff["change_kind"]
    file_ids = diff["file_id"]
    changed_mask = pc.is_in(change_kind, value_set=_CHANGED_KIND_SET)
    deleted_mask = pc.is_in(change_kind, value_set=_DELETED_KIND_SET)
    changed = _unique_file_ids(file_ids, changed_mask)
    deleted = _unique_file_ids(file_ids, deleted_mask)
    unchanged_mask = pc.equal(change_kind, pa.scalar("unchanged", type=pa.string()))
    unchanged_any = pc.any(unchanged_mask).as_py()
    full_refresh = not bool(unchanged_any)
    return IncrementalFileChanges(
        changed_file_ids=changed,
        deleted_file_ids=deleted,
        full_refresh=full_refresh,
    )


def _unique_file_ids(
    file_ids: pa.Array | pa.ChunkedArray,
    mask: pa.Array | pa.ChunkedArray,
) -> tuple[str, ...]:
    filtered = pc.filter(file_ids, mask)
    values = _values_as_list(filtered)
    cleaned = [value for value in values if isinstance(value, str) and value]
    return tuple(sorted(set(cleaned)))


def _values_as_list(values: pa.Array | pa.ChunkedArray) -> list[object]:
    if isinstance(values, pa.ChunkedArray):
        combined = values.combine_chunks()
        return combined.to_pylist()
    return values.to_pylist()


__all__ = ["file_changes_from_diff"]
