"""Delta CDF diff helpers for incremental pipeline runs."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from arrowdsl.schema.serialization import schema_fingerprint
from incremental.cdf_cursors import CdfCursorStore
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_runtime import read_cdf_changes
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from incremental.streaming_writes import StreamingWriteOptions, stream_table_to_delta
from storage.deltalake import (
    DeltaWriteOptions,
    DeltaWriteResult,
    enable_delta_features,
    write_table_delta,
)

_STREAMING_ROW_THRESHOLD = 250_000


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
    runtime : IncrementalRuntime
        Shared incremental runtime for DataFusion execution.

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
    result = read_cdf_changes(
        runtime,
        dataset_path=dataset_path,
        dataset_name=dataset_name,
        cursor_store=cursor_store,
        filter_policy=filter_policy,
    )
    if result is None:
        return None
    return result.table


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
    options = DeltaWriteOptions(
        mode="overwrite",
        schema_mode="overwrite",
        commit_metadata={
            "snapshot_kind": "incremental_diff",
            "schema_fingerprint": schema_fingerprint(diff.schema),
        },
    )
    if diff.num_rows >= _STREAMING_ROW_THRESHOLD:
        stream_table_to_delta(
            diff,
            str(target),
            options=StreamingWriteOptions(),
            write_options=options,
        )
        result = DeltaWriteResult(
            path=str(target),
            version=DeltaTable(str(target)).version(),
        )
    else:
        result = write_table_delta(diff, str(target), options=options)
    enable_delta_features(result.path)
    return result


__all__ = [
    "diff_snapshots_with_delta_cdf",
    "write_incremental_diff",
]
