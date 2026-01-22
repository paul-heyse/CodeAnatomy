"""Incremental change derivation helpers."""

from __future__ import annotations

import ibis

from incremental.cdf_filters import CdfChangeType
from incremental.cdf_runtime import CdfReadResult
from incremental.ibis_exec import ibis_expr_to_table
from incremental.ibis_utils import ibis_table_from_arrow
from incremental.runtime import IncrementalRuntime
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

    backend = runtime.ibis_backend()
    cdf_expr = ibis_table_from_arrow(backend, cdf_result.table)
    insert_type = ibis.literal(CdfChangeType.INSERT.to_cdf_column_value())
    update_type = ibis.literal(CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value())
    delete_type = ibis.literal(CdfChangeType.DELETE.to_cdf_column_value())
    change_type_col = cdf_expr["_change_type"]
    file_id_col = cdf_expr[file_id_column]

    changed_filter = ibis.and_(
        change_type_col.isin([insert_type, update_type]),
        file_id_col.notnull(),
    )
    changed_expr = cdf_expr.filter(changed_filter).select(file_id=file_id_col).distinct()
    changed_table = ibis_expr_to_table(
        changed_expr,
        runtime=runtime,
        name="cdf_changed_file_ids",
    )
    changed = tuple(
        sorted({value for value in changed_table["file_id"].to_pylist() if isinstance(value, str)})
    )

    deleted_filter = ibis.and_(
        change_type_col.isin([delete_type]),
        file_id_col.notnull(),
    )
    deleted_expr = cdf_expr.filter(deleted_filter).select(file_id=file_id_col).distinct()
    deleted_table = ibis_expr_to_table(
        deleted_expr,
        runtime=runtime,
        name="cdf_deleted_file_ids",
    )
    deleted = tuple(
        sorted({value for value in deleted_table["file_id"].to_pylist() if isinstance(value, str)})
    )

    # CDF always represents a delta, not a full refresh
    return IncrementalFileChanges(
        changed_file_ids=changed,
        deleted_file_ids=deleted,
        full_refresh=False,
    )


__all__ = ["file_changes_from_cdf"]
