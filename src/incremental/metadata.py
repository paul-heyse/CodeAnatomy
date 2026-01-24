"""Incremental metadata and diagnostics artifact persistence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import pyarrow as pa

from arrowdsl.schema.build import table_from_schema
from engine.runtime_profile import runtime_profile_snapshot
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from incremental.cdf_cursors import CdfCursorStore
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from serde_msgspec import to_builtins
from sqlglot_tools.optimizer import sqlglot_policy_snapshot_for
from storage.deltalake import StorageOptions, enable_delta_features

_CDF_CURSOR_SCHEMA = pa.schema(
    [
        pa.field("dataset_name", pa.string(), nullable=False),
        pa.field("last_version", pa.int64(), nullable=False),
    ]
)


def write_incremental_metadata(
    state_store: StateStore,
    *,
    runtime: IncrementalRuntime,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> str:
    """Persist runtime + SQLGlot policy metadata to the state store.

    Returns
    -------
    str
        Delta table path for the metadata snapshot.
    """
    state_store.ensure_dirs()
    runtime_snapshot = runtime_profile_snapshot(runtime.execution_ctx.runtime)
    snapshot = sqlglot_policy_snapshot_for(runtime.sqlglot_policy)
    payload = {
        "datafusion_settings_hash": runtime.profile.settings_hash(),
        "runtime_profile_hash": runtime_snapshot.profile_hash,
        "runtime_profile": runtime_snapshot.payload(),
        "sqlglot_policy_hash": snapshot.policy_hash,
        "sqlglot_policy": snapshot.payload(),
    }
    table = pa.Table.from_pylist([payload])
    path = state_store.incremental_metadata_path()
    result = write_ibis_dataset_delta(
        table,
        str(path),
        options=IbisDatasetWriteOptions(
            execution=runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata={"snapshot_kind": "incremental_metadata"},
                storage_options=storage_options,
                log_storage_options=log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return result.path


def write_cdf_cursor_snapshot(
    state_store: StateStore,
    *,
    cursor_store: CdfCursorStore,
    runtime: IncrementalRuntime,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> str:
    """Persist the current CDF cursor snapshot to the state store.

    Parameters
    ----------
    state_store : StateStore
        State store for incremental metadata.
    cursor_store : CdfCursorStore
        Cursor store supplying snapshot rows.
    runtime : IncrementalRuntime
        Runtime used for Delta write execution.
    storage_options : StorageOptions | None
        Optional storage options for Delta access.
    log_storage_options : StorageOptions | None
        Optional log-storage options for Delta access.

    Returns
    -------
    str
        Delta table path for the cursor snapshot.
    """
    state_store.ensure_dirs()
    cursors = cursor_store.list_cursors()
    if cursors:
        rows = [cast("dict[str, object]", to_builtins(cursor)) for cursor in cursors]
        table = pa.Table.from_pylist(rows, schema=_CDF_CURSOR_SCHEMA)
    else:
        table = table_from_schema(_CDF_CURSOR_SCHEMA, columns={}, num_rows=0)
    path = state_store.cdf_cursor_snapshot_path()
    result = write_ibis_dataset_delta(
        table,
        str(path),
        options=IbisDatasetWriteOptions(
            execution=runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata={"snapshot_kind": "cdf_cursor_snapshot"},
                storage_options=storage_options,
                log_storage_options=log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return result.path


def write_incremental_artifacts(
    state_store: StateStore,
    *,
    runtime: IncrementalRuntime,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> dict[str, str]:
    """Persist selected incremental diagnostics artifacts to Delta.

    Returns
    -------
    dict[str, str]
        Mapping of artifact names to Delta table paths.
    """
    updated: dict[str, str] = {}
    artifact_map = {
        "incremental_file_pruning_v1": state_store.pruning_metrics_path(),
        "incremental_sqlglot_plan_v1": state_store.sqlglot_artifacts_path(),
    }
    for name, path in artifact_map.items():
        result = _write_artifact_table(
            name=name,
            path=path,
            runtime=runtime,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        if result is not None:
            updated[name] = result
    return updated


def _write_artifact_table(
    *,
    name: str,
    path: Path,
    runtime: IncrementalRuntime,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> str | None:
    sink = runtime.profile.diagnostics_sink
    if sink is None:
        return None
    artifacts = sink.artifacts_snapshot().get(name)
    if not artifacts:
        return None
    table = _artifacts_to_table(artifacts)
    result = write_ibis_dataset_delta(
        table,
        str(path),
        options=IbisDatasetWriteOptions(
            execution=runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata={"artifact_name": name},
                storage_options=storage_options,
                log_storage_options=log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return result.path


def _artifacts_to_table(artifacts: Sequence[Mapping[str, object]]) -> pa.Table:
    return pa.Table.from_pylist([dict(row) for row in artifacts])


__all__ = [
    "write_cdf_cursor_snapshot",
    "write_incremental_artifacts",
    "write_incremental_metadata",
]
