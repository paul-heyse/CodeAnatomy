"""Incremental change derivation helpers."""

from __future__ import annotations

import pyarrow as pa

from incremental.cdf_filters import CdfChangeType
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from incremental.types import IncrementalFileChanges


def file_changes_from_cdf(
    cdf: pa.Table | None,
    *,
    runtime: IncrementalRuntime,
) -> IncrementalFileChanges:
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

    ctx = runtime.session_context()
    sql_options = runtime.profile.sql_options()
    with TempTableRegistry(ctx) as registry:
        cdf_name = registry.register_table(cdf, prefix="cdf")
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


__all__ = ["file_changes_from_cdf"]
