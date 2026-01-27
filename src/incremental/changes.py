"""Incremental change derivation helpers."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from datafusion_engine.arrow_ingest import datafusion_from_arrow

if TYPE_CHECKING:
    from datafusion.expr import Expr
from incremental.cdf_filters import CdfChangeType
from incremental.cdf_runtime import CdfReadResult
from incremental.plan_bundle_exec import execute_df_to_table
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

    ctx = runtime.session_runtime().ctx
    with TempTableRegistry(runtime) as registry:
        cdf_name = f"__cdf_changes_{uuid.uuid4().hex}"
        _ = datafusion_from_arrow(ctx, name=cdf_name, value=cdf_result.table)
        registry.track(cdf_name)
        df = ctx.table(cdf_name)
        changed = _collect_file_ids(
            df,
            runtime=runtime,
            view_name="incremental_cdf_changed_files",
            change_types=(
                CdfChangeType.INSERT.to_cdf_column_value(),
                CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value(),
            ),
            file_id_column=file_id_column,
        )
        deleted = _collect_file_ids(
            df,
            runtime=runtime,
            view_name="incremental_cdf_deleted_files",
            change_types=(CdfChangeType.DELETE.to_cdf_column_value(),),
            file_id_column=file_id_column,
        )

    # CDF always represents a delta, not a full refresh
    return IncrementalFileChanges(
        changed_file_ids=changed,
        deleted_file_ids=deleted,
        full_refresh=False,
    )


def _collect_file_ids(
    df: DataFrame,
    *,
    runtime: IncrementalRuntime,
    view_name: str,
    change_types: tuple[str, ...],
    file_id_column: str,
) -> tuple[str, ...]:
    """Collect distinct file IDs for the requested CDF change types.

    Returns
    -------
    tuple[str, ...]
        Sorted, unique file identifiers.
    """
    file_id_col = col(file_id_column)
    predicate = _cdf_change_predicate(change_types) & file_id_col.is_not_null()
    filtered = df.filter(predicate).select(file_id_col.alias("file_id")).distinct()
    table = execute_df_to_table(runtime, filtered, view_name=view_name)
    values = {value for value in table["file_id"].to_pylist() if isinstance(value, str)}
    return tuple(sorted(values))


def _cdf_change_predicate(change_types: tuple[str, ...]) -> Expr:
    """Build CDF change type predicate using UDF helpers.

    Uses cdf_is_upsert and cdf_is_delete UDFs for semantic clarity.
    Falls back to in_list for mixed or custom change types.

    Returns
    -------
    Expr
        Filter predicate for the specified CDF change types.
    """
    from datafusion_ext import cdf_is_delete, cdf_is_upsert

    change_type_col = col("_change_type")
    upsert_types = {
        CdfChangeType.INSERT.to_cdf_column_value(),
        CdfChangeType.UPDATE_POSTIMAGE.to_cdf_column_value(),
    }
    delete_types = {CdfChangeType.DELETE.to_cdf_column_value()}
    change_set = set(change_types)

    if change_set == upsert_types:
        return cdf_is_upsert(change_type_col)
    if change_set == delete_types:
        return cdf_is_delete(change_type_col)
    if change_set <= upsert_types | delete_types:
        has_upsert = bool(change_set & upsert_types)
        has_delete = bool(change_set & delete_types)
        if has_upsert and has_delete:
            return cdf_is_upsert(change_type_col) | cdf_is_delete(change_type_col)
        if has_upsert:
            return cdf_is_upsert(change_type_col)
        if has_delete:
            return cdf_is_delete(change_type_col)
    # Fallback to in_list for custom or unknown change types
    change_literals = [lit(value) for value in change_types]
    return f.in_list(change_type_col, change_literals)


__all__ = ["file_changes_from_cdf"]
