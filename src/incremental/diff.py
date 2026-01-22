"""Delta CDF diff helpers for incremental pipeline runs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from deltalake import DeltaTable

from arrowdsl.schema.serialization import schema_fingerprint
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import CdfReadResult, read_cdf_changes
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from storage.deltalake import enable_delta_features

if TYPE_CHECKING:
    from storage.deltalake import DeltaWriteResult


def diff_snapshots_with_delta_cdf(
    *,
    dataset_path: str,
    cursor_store: CdfCursorStore,
    dataset_name: str,
    filter_policy: CdfFilterPolicy | None = None,
    runtime: IncrementalRuntime,
) -> CdfReadResult | None:
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
    runtime : IncrementalRuntime
        Shared incremental runtime for DataFusion execution.

    Returns
    -------
    CdfReadResult | None
        CDF changes result, or None if CDF is not available.

    """
    # Check if Delta table exists
    if not Path(dataset_path).exists():
        return None

    # Check if it's a Delta table
    if not DeltaTable.is_deltatable(dataset_path):
        return None
    return read_cdf_changes(
        runtime,
        dataset_path=dataset_path,
        dataset_name=dataset_name,
        cursor_store=cursor_store,
        filter_policy=filter_policy,
    )


def write_incremental_diff(
    store: StateStore,
    diff: pa.Table,
    *,
    runtime: IncrementalRuntime,
) -> DeltaWriteResult:
    """Persist the incremental diff to the state store as Delta.

    Parameters
    ----------
    store : StateStore
        State store holding incremental dataset paths.
    diff : pa.Table
        Incremental diff table to write.
    runtime : IncrementalRuntime
        Runtime used for Delta write execution and diagnostics.

    Returns
    -------
    str
        Path to the written diff Delta table.
    """
    store.ensure_dirs()
    target = store.incremental_diff_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    commit_metadata = {
        "snapshot_kind": "incremental_diff",
        "schema_fingerprint": schema_fingerprint(diff.schema),
    }
    result = write_ibis_dataset_delta(
        diff,
        str(target),
        options=IbisDatasetWriteOptions(
            execution=runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata=commit_metadata,
            ),
        ),
    )
    enable_delta_features(result.path)
    return result


__all__ = [
    "diff_snapshots_with_delta_cdf",
    "write_incremental_diff",
]
