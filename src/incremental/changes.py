"""Incremental change derivation helpers."""

from __future__ import annotations

import contextlib
import uuid

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.types import IncrementalFileChanges

_CHANGED_KINDS: tuple[str, ...] = ("added", "modified", "renamed")
_DELETED_KINDS: tuple[str, ...] = ("deleted",)


def _session_context() -> SessionContext:
    return DataFusionRuntimeProfile().session_context()


def _register_table(ctx: SessionContext, table: pa.Table, *, prefix: str) -> str:
    name = f"__diff_changes_{prefix}_{uuid.uuid4().hex}"
    ctx.register_record_batches(name, table.to_batches())
    return name


def _deregister_table(ctx: SessionContext, name: str | None) -> None:
    if name is None:
        return
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


def _distinct_file_ids(
    ctx: SessionContext, *, table_name: str, kinds: tuple[str, ...]
) -> tuple[str, ...]:
    if not kinds:
        return ()
    literals = ", ".join(f"'{kind}'" for kind in kinds)
    table = ctx.sql(
        f"SELECT DISTINCT file_id FROM {table_name} WHERE change_kind IN ({literals})"
    ).to_arrow_table()
    values = [value for value in table["file_id"].to_pylist() if isinstance(value, str) and value]
    return tuple(sorted(set(values)))


def _has_kind(ctx: SessionContext, *, table_name: str, kind: str) -> bool:
    table = ctx.sql(
        f"SELECT COUNT(*) AS count FROM {table_name} WHERE change_kind = '{kind}'"
    ).to_arrow_table()
    value = table["count"][0].as_py() if table.num_rows else 0
    return bool(value)


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
    ctx = _session_context()
    diff_name = _register_table(ctx, diff, prefix="diff")
    try:
        changed = _distinct_file_ids(ctx, table_name=diff_name, kinds=_CHANGED_KINDS)
        deleted = _distinct_file_ids(ctx, table_name=diff_name, kinds=_DELETED_KINDS)
        unchanged_any = _has_kind(ctx, table_name=diff_name, kind="unchanged")
    finally:
        _deregister_table(ctx, diff_name)
    return IncrementalFileChanges(
        changed_file_ids=changed,
        deleted_file_ids=deleted,
        full_refresh=not unchanged_any,
    )


__all__ = ["file_changes_from_diff"]
