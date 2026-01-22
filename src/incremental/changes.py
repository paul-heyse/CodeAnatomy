"""Incremental change derivation helpers."""

from __future__ import annotations

import contextlib
import uuid

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.cdf_filters import CdfChangeType
from incremental.types import IncrementalFileChanges

_CHANGED_KINDS: tuple[str, ...] = ("added", "modified", "renamed")
_DELETED_KINDS: tuple[str, ...] = ("deleted",)


def _session_profile() -> DataFusionRuntimeProfile:
    return DataFusionRuntimeProfile()


def _session_context(profile: DataFusionRuntimeProfile) -> SessionContext:
    return profile.session_context()


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
    ctx: SessionContext,
    *,
    table_name: str,
    kinds: tuple[str, ...],
    sql_options: SQLOptions,
) -> tuple[str, ...]:
    if not kinds:
        return ()
    literals = ", ".join(f"'{kind}'" for kind in kinds)
    sql = f"SELECT DISTINCT file_id FROM {table_name} WHERE change_kind IN ({literals})"
    table = ctx.sql_with_options(sql, sql_options).to_arrow_table()
    values = [value for value in table["file_id"].to_pylist() if isinstance(value, str) and value]
    return tuple(sorted(set(values)))


def _has_kind(ctx: SessionContext, *, table_name: str, kind: str, sql_options: SQLOptions) -> bool:
    sql = f"SELECT COUNT(*) AS count FROM {table_name} WHERE change_kind = '{kind}'"
    table = ctx.sql_with_options(sql, sql_options).to_arrow_table()
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
    profile = _session_profile()
    ctx = _session_context(profile)
    sql_options = profile.sql_options()
    diff_name = _register_table(ctx, diff, prefix="diff")
    try:
        changed = _distinct_file_ids(
            ctx,
            table_name=diff_name,
            kinds=_CHANGED_KINDS,
            sql_options=sql_options,
        )
        deleted = _distinct_file_ids(
            ctx,
            table_name=diff_name,
            kinds=_DELETED_KINDS,
            sql_options=sql_options,
        )
        unchanged_any = _has_kind(
            ctx,
            table_name=diff_name,
            kind="unchanged",
            sql_options=sql_options,
        )
    finally:
        _deregister_table(ctx, diff_name)
    return IncrementalFileChanges(
        changed_file_ids=changed,
        deleted_file_ids=deleted,
        full_refresh=not unchanged_any,
    )


def file_changes_from_cdf(cdf: pa.Table | None) -> IncrementalFileChanges:
    """Derive file change sets from Delta CDF table.

    Parameters
    ----------
    cdf:
        CDF table with _change_type column.

    Returns
    -------
    IncrementalFileChanges
        Change sets derived from CDF change types.
    """
    if cdf is None or cdf.num_rows == 0:
        return IncrementalFileChanges()

    profile = _session_profile()
    ctx = _session_context(profile)
    sql_options = profile.sql_options()
    cdf_name = _register_table(ctx, cdf, prefix="cdf")

    try:
        # Extract changed file IDs (inserts and updates)
        insert_type = CdfChangeType.INSERT.to_cdf_column_value()
        update_type = CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value()
        changed_sql = f"""
            SELECT DISTINCT file_id
            FROM {cdf_name}
            WHERE _change_type IN ('{insert_type}', '{update_type}')
            AND file_id IS NOT NULL
        """
        changed_table = ctx.sql_with_options(changed_sql, sql_options).to_arrow_table()
        changed = tuple(
            sorted(
                {value for value in changed_table["file_id"].to_pylist() if isinstance(value, str)}
            )
        )

        # Extract deleted file IDs
        delete_type = CdfChangeType.DELETE.to_cdf_column_value()
        deleted_sql = f"""
            SELECT DISTINCT file_id
            FROM {cdf_name}
            WHERE _change_type = '{delete_type}'
            AND file_id IS NOT NULL
        """
        deleted_table = ctx.sql_with_options(deleted_sql, sql_options).to_arrow_table()
        deleted = tuple(
            sorted(
                {value for value in deleted_table["file_id"].to_pylist() if isinstance(value, str)}
            )
        )

        # CDF always represents a delta, not a full refresh
        return IncrementalFileChanges(
            changed_file_ids=changed,
            deleted_file_ids=deleted,
            full_refresh=False,
        )
    finally:
        _deregister_table(ctx, cdf_name)


__all__ = ["file_changes_from_cdf", "file_changes_from_diff"]
