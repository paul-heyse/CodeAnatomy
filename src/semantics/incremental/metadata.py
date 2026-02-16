"""Incremental metadata and diagnostics artifact persistence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from datafusion_engine.arrow.build import empty_table
from datafusion_engine.arrow.schema import dataset_name_field
from datafusion_engine.delta.store_policy import resolve_delta_store_policy
from datafusion_engine.io.write_core import WriteMode
from datafusion_engine.views.artifacts import view_artifact_payload_table
from extraction.runtime_profile import runtime_profile_snapshot
from semantics.incremental.cdf_cursors import CdfCursorStore
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.state_store import StateStore
from semantics.incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)
from serde_artifacts import IncrementalMetadataSnapshot
from serde_msgspec import to_builtins
from storage.deltalake import (
    StorageOptions,
)

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import TableLike

_CDF_CURSOR_SCHEMA = pa.schema(
    [
        dataset_name_field(),
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
    """Persist runtime metadata to the state store.

    Returns:
    -------
    str
        Delta table path for the metadata snapshot.
    """
    state_store.ensure_dirs()
    runtime_snapshot = runtime_profile_snapshot(
        runtime.profile,
        name=runtime.config_policy_name(),
        determinism_tier=runtime.determinism_tier,
    )
    payload = IncrementalMetadataSnapshot(
        datafusion_settings_hash=runtime.settings_hash(),
        runtime_profile_hash=runtime_snapshot.profile_hash,
        runtime_profile=runtime_snapshot,
    )
    table = pa.Table.from_pylist([cast("dict[str, object]", to_builtins(payload))])
    path = state_store.incremental_metadata_path()
    write_delta_table_via_pipeline(
        runtime=runtime,
        table=table,
        request=IncrementalDeltaWriteRequest(
            destination=str(path),
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata={"snapshot_kind": "incremental_metadata"},
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            operation_id="incremental_metadata",
        ),
    )
    return str(path)


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

    Returns:
    -------
    str
        Delta table path for the cursor snapshot.
    """
    _ = runtime
    state_store.ensure_dirs()
    cursors = cursor_store.list_cursors()
    if cursors:
        rows = [cast("dict[str, object]", to_builtins(cursor)) for cursor in cursors]
        table = pa.Table.from_pylist(rows, schema=_CDF_CURSOR_SCHEMA)
    else:
        table = empty_table(_CDF_CURSOR_SCHEMA)
    path = state_store.cdf_cursor_snapshot_path()
    write_delta_table_via_pipeline(
        runtime=runtime,
        table=table,
        request=IncrementalDeltaWriteRequest(
            destination=str(path),
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata={"snapshot_kind": "cdf_cursor_snapshot"},
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            operation_id="incremental_cdf_cursor_snapshot",
        ),
    )
    return str(path)


@dataclass(frozen=True)
class ArtifactWriteContext:
    """Shared write configuration for incremental artifacts."""

    runtime: IncrementalRuntime
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None

    def resolve_storage(self, *, table_uri: str) -> tuple[dict[str, str], dict[str, str]]:
        """Return storage and log-store options for the artifact table.

        Returns:
        -------
        tuple[dict[str, str], dict[str, str]]
            Resolved storage and log-store options.
        """
        storage, log_storage = resolve_delta_store_policy(
            table_uri=table_uri,
            policy=self.runtime.delta_store_policy(),
            storage_options=self.storage_options,
            log_storage_options=self.log_storage_options,
        )
        return storage, log_storage


@dataclass(frozen=True)
class SemanticDiagnosticsSnapshot:
    """Snapshot payload for semantic diagnostics persistence."""

    name: str
    table: TableLike | pa.Table | None
    destination: Path


def write_semantic_diagnostics_snapshots(
    *,
    runtime: IncrementalRuntime,
    snapshots: Mapping[str, SemanticDiagnosticsSnapshot],
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> dict[str, str]:
    """Persist semantic diagnostics snapshots to Delta.

    Returns:
    -------
    dict[str, str]
        Mapping of snapshot names to Delta table paths.
    """
    updated: dict[str, str] = {}
    context = ArtifactWriteContext(
        runtime=runtime,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    for name, snapshot in snapshots.items():
        table = snapshot.table
        if table is None:
            table = empty_table(pa.schema([]))
        snapshot.destination.parent.mkdir(parents=True, exist_ok=True)
        storage, log_storage = context.resolve_storage(table_uri=str(snapshot.destination))
        write_delta_table_via_pipeline(
            runtime=runtime,
            table=table,
            request=IncrementalDeltaWriteRequest(
                destination=str(snapshot.destination),
                mode=WriteMode.OVERWRITE,
                schema_mode="overwrite",
                commit_metadata={"snapshot_kind": name},
                storage_options=storage,
                log_storage_options=log_storage,
                operation_id=f"semantic_diagnostics::{name}",
            ),
        )
        updated[name] = str(snapshot.destination)
    return updated


def write_incremental_artifacts(
    state_store: StateStore,
    *,
    runtime: IncrementalRuntime,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> dict[str, str]:
    """Persist selected incremental diagnostics artifacts to Delta.

    Returns:
    -------
    dict[str, str]
        Mapping of artifact names to Delta table paths.
    """
    updated: dict[str, str] = {}
    context = ArtifactWriteContext(
        runtime=runtime,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    artifact_map = {
        "incremental_file_pruning_v1": state_store.pruning_metrics_path(),
    }
    for name, path in artifact_map.items():
        result = _write_artifact_table(
            name=name,
            path=path,
            context=context,
        )
        if result is not None:
            updated[name] = result
    view_snapshot = runtime.profile.view_registry_snapshot()
    if view_snapshot:
        view_path = state_store.view_artifacts_path()
        result = _write_view_artifact_rows(
            name="incremental_view_artifacts_v1",
            path=view_path,
            rows=view_snapshot,
            context=context,
        )
        if result is not None:
            updated["incremental_view_artifacts_v1"] = result
    return updated


def _write_artifact_rows(
    *,
    name: str,
    path: Path,
    rows: Sequence[Mapping[str, object]],
    context: ArtifactWriteContext,
) -> str | None:
    if not rows:
        return None
    table = _artifacts_to_table(rows)
    return _write_named_artifact(name=name, path=path, table=table, context=context)


def _write_view_artifact_rows(
    *,
    name: str,
    path: Path,
    rows: Sequence[Mapping[str, object]],
    context: ArtifactWriteContext,
) -> str | None:
    if not rows:
        return None
    table = view_artifact_payload_table(rows)
    return _write_named_artifact(
        name=name,
        path=path,
        table=table,
        context=context,
        operation_prefix="incremental_view_artifacts",
    )


def _write_artifact_table(
    *,
    name: str,
    path: Path,
    context: ArtifactWriteContext,
) -> str | None:
    sink = context.runtime.diagnostics_sink()
    if sink is None:
        return None
    artifacts = sink.artifacts_snapshot().get(name)
    if not artifacts:
        return None
    table = _artifacts_to_table(artifacts)
    return _write_named_artifact(name=name, path=path, table=table, context=context)


def _write_named_artifact(
    *,
    name: str,
    path: Path,
    table: pa.Table,
    context: ArtifactWriteContext,
    operation_prefix: str = "incremental_artifact",
) -> str:
    """Write a named incremental artifact as an overwrite Delta table.

    Returns:
        str: Written artifact URI/path.
    """
    storage_options, log_storage_options = context.resolve_storage(table_uri=str(path))
    write_delta_table_via_pipeline(
        runtime=context.runtime,
        table=table,
        request=IncrementalDeltaWriteRequest(
            destination=str(path),
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata={"artifact_name": name},
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            operation_id=f"{operation_prefix}::{name}",
        ),
    )
    return str(path)


def _artifacts_to_table(artifacts: Sequence[Mapping[str, object]]) -> pa.Table:
    return pa.Table.from_pylist([dict(row) for row in artifacts])


__all__ = [
    "SemanticDiagnosticsSnapshot",
    "write_cdf_cursor_snapshot",
    "write_incremental_artifacts",
    "write_incremental_metadata",
    "write_semantic_diagnostics_snapshots",
]
