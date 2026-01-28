"""Delta CDF diff helpers for incremental pipeline runs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow_schema.abi import schema_fingerprint
from datafusion_engine.write_pipeline import WriteMode
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import CdfReadResult, read_cdf_changes
from incremental.delta_context import DeltaAccessContext
from incremental.state_store import StateStore
from incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)
from storage.deltalake import delta_table_version

if TYPE_CHECKING:
    from storage.deltalake import DeltaWriteResult


def diff_snapshots_with_delta_cdf(
    context: DeltaAccessContext,
    *,
    dataset_path: str,
    cursor_store: CdfCursorStore,
    dataset_name: str,
    filter_policy: CdfFilterPolicy | None = None,
) -> CdfReadResult | None:
    """Diff snapshots using Delta CDF for efficient incremental reads.

    This function reads changes from a Delta table's change data feed
    starting from the last processed version tracked by the cursor store.

    Parameters
    ----------
    context : DeltaAccessContext
        Delta access context supplying runtime and storage options.
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
    CdfReadResult | None
        CDF changes result, or None if CDF is not available.

    """
    # Check if Delta table exists
    if not Path(dataset_path).exists():
        return None

    # Check if it's a Delta table
    resolved = context.resolve_storage(table_uri=dataset_path)
    if (
        delta_table_version(
            dataset_path,
            storage_options=resolved.storage_options,
            log_storage_options=resolved.log_storage_options,
        )
        is None
    ):
        return None
    return read_cdf_changes(
        context,
        dataset_path=dataset_path,
        dataset_name=dataset_name,
        cursor_store=cursor_store,
        filter_policy=filter_policy,
    )


def write_incremental_diff(
    store: StateStore,
    diff: pa.Table,
    *,
    context: DeltaAccessContext,
) -> DeltaWriteResult:
    """Persist the incremental diff to the state store as Delta.

    Parameters
    ----------
    store : StateStore
        State store holding incremental dataset paths.
    diff : pa.Table
        Incremental diff table to write.
    context : DeltaAccessContext
        Delta access context supplying runtime and storage options.

    Returns
    -------
    str
        Path to the written diff Delta table.

    Raises
    ------
    RuntimeError
        Raised when the Delta write result is unavailable.
    """
    store.ensure_dirs()
    target = store.incremental_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    commit_metadata = {
        "snapshot_kind": "incremental_diff",
        "schema_fingerprint": schema_fingerprint(diff.schema),
    }
    resolved_storage = context.resolve_storage(table_uri=str(target))
    write_result = write_delta_table_via_pipeline(
        runtime=context.runtime,
        table=diff,
        request=IncrementalDeltaWriteRequest(
            destination=str(target),
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata=commit_metadata,
            storage_options=resolved_storage.storage_options,
            log_storage_options=resolved_storage.log_storage_options,
            operation_id="incremental_diff",
        ),
    )
    if write_result.delta_result is None:
        msg = "Incremental diff Delta write did not return a result."
        raise RuntimeError(msg)
    return write_result.delta_result


__all__ = [
    "diff_snapshots_with_delta_cdf",
    "write_incremental_diff",
]
