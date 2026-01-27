"""Incremental change derivation helpers."""

from __future__ import annotations

import uuid

from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.arrow_ingest import datafusion_from_arrow
from incremental.cdf_filters import CdfChangeType
from incremental.cdf_runtime import CdfReadResult
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from incremental.types import IncrementalFileChanges


def file_changes_from_cdf(
    cdf_result: CdfReadResult | None,
    *,
    runtime: IncrementalRuntime,
    file_id_column: str = "file_id",
) -> IncrementalFileChanges:
    """Derive file change sets from Delta CDF table.

    Parameters
    ----------
    cdf_result:
        CDF read result with _change_type column.
    runtime : IncrementalRuntime
        Shared incremental runtime for DataFusion execution.
    file_id_column : str
        Column name to use for file identifiers in the CDF table.

    Returns
    -------
    IncrementalFileChanges
        Change sets derived from CDF change types.
    """
    if cdf_result is None or cdf_result.table.num_rows == 0:
        return IncrementalFileChanges()

    ctx = runtime.session_context()
    with TempTableRegistry(runtime) as registry:
        cdf_name = f"__cdf_changes_{uuid.uuid4().hex}"
        _ = datafusion_from_arrow(ctx, name=cdf_name, value=cdf_result.table)
        registry.track(cdf_name)
        df = ctx.table(cdf_name)
        change_type_col = col("_change_type")
        file_id_col = col(file_id_column)
        insert_type = lit(CdfChangeType.INSERT.to_cdf_column_value())
        update_type = lit(CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value())
        delete_type = lit(CdfChangeType.DELETE.to_cdf_column_value())

        changed_filter = (
            f.in_list(change_type_col, [insert_type, update_type]) & file_id_col.is_not_null()
        )
        changed_table = (
            df.filter(changed_filter)
            .select(file_id_col.alias("file_id"))
            .distinct()
            .to_arrow_table()
        )
        changed_values = {
            value
            for value in changed_table["file_id"].to_pylist()
            if isinstance(value, str)
        }
        changed = tuple(sorted(changed_values))

        deleted_filter = (
            f.in_list(change_type_col, [delete_type]) & file_id_col.is_not_null()
        )
        deleted_table = (
            df.filter(deleted_filter)
            .select(file_id_col.alias("file_id"))
            .distinct()
            .to_arrow_table()
        )
        deleted_values = {
            value
            for value in deleted_table["file_id"].to_pylist()
            if isinstance(value, str)
        }
        deleted = tuple(sorted(deleted_values))

    # CDF always represents a delta, not a full refresh
    return IncrementalFileChanges(
        changed_file_ids=changed,
        deleted_file_ids=deleted,
        full_refresh=False,
    )


__all__ = ["file_changes_from_cdf"]
