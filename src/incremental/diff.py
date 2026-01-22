"""Delta CDF diff helpers for incremental pipeline runs."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from arrowdsl.schema.serialization import schema_fingerprint
from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaCdfOptions,
    DeltaWriteOptions,
    DeltaWriteResult,
    enable_delta_features,
    write_table_delta,
)
from storage.deltalake.delta import read_delta_cdf


def diff_snapshots_with_delta_cdf(
    *,
    dataset_path: str,
    cursor_store: CdfCursorStore,
    dataset_name: str,
    filter_policy: CdfFilterPolicy | None = None,
    runtime: IncrementalRuntime,
) -> pa.Table | None:
    """Diff snapshots using Delta CDF for efficient incremental reads.

    This function reads changes from a Delta table's change data feed
    starting from the last processed version tracked by the cursor store.

    Parameters
    ----------
    dataset_path : str
        Path to the Delta table.
    cursor_store : CdfCursorStore
        Store managing CDF cursors.
    dataset_name : str
        Name of the dataset for cursor tracking.
    filter_policy : CdfFilterPolicy | None
        Policy for filtering change types. If None, includes all changes.

    Returns
    -------
    pa.Table | None
        CDF changes table, or None if CDF is not available.

    """
    # Check if Delta table exists
    if not Path(dataset_path).exists():
        return None

    # Check if it's a Delta table
    if not DeltaTable.is_deltatable(dataset_path):
        return None

    # Get current cursor
    cursor = cursor_store.load_cursor(dataset_name)

    # Open Delta table to get current version
    dt = DeltaTable(dataset_path)
    current_version = dt.version()

    # If no cursor exists, return None to signal full snapshot comparison
    if cursor is None:
        # Create initial cursor at current version
        cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))
        return None

    # If versions are the same, no changes
    if cursor.last_version >= current_version:
        return None

    # Read CDF changes
    starting_version = cursor.last_version + 1
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
    )

    try:
        cdf_table = read_delta_cdf(
            dataset_path,
            cdf_options=cdf_options,
        )
    except ValueError:
        # CDF not enabled - fall back to None to signal full comparison
        return None

    # Apply filter policy if provided
    if filter_policy is not None:
        predicate = filter_policy.to_sql_predicate()
        if predicate is not None and predicate != "FALSE":
            ctx = runtime.session_context()
            sql_options = runtime.profile.sql_options()
            with TempTableRegistry(ctx) as registry:
                temp_name = registry.register_table(cdf_table, prefix="cdf_filter")
                filtered_df = ctx.sql_with_options(
                    f"SELECT * FROM {temp_name} WHERE {predicate}",
                    sql_options,
                )
                cdf_table = filtered_df.to_arrow_table()
        elif predicate == "FALSE":
            # No changes match filter
            cdf_table = pa.table({}, schema=cdf_table.schema)

    # Update cursor to current version
    cursor_store.save_cursor(CdfCursor(dataset_name=dataset_name, last_version=current_version))

    return cdf_table


def write_incremental_diff(store: StateStore, diff: pa.Table) -> DeltaWriteResult:
    """Persist the incremental diff to the state store as Delta.

    Returns
    -------
    str
        Path to the written diff Delta table.
    """
    store.ensure_dirs()
    target = store.incremental_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    result = write_table_delta(
        diff,
        str(target),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={
                "snapshot_kind": "incremental_diff",
                "schema_fingerprint": schema_fingerprint(diff.schema),
            },
        ),
    )
    enable_delta_features(result.path)
    return result


__all__ = [
    "diff_snapshots_with_delta_cdf",
    "write_incremental_diff",
]
