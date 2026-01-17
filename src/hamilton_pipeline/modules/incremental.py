"""Incremental pipeline nodes for snapshots, diffs, and updates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from hamilton.function_modifiers import tag

from arrowdsl.core.interop import TableLike
from hamilton_pipeline.pipeline_types import RelationshipOutputTables
from incremental.changes import file_changes_from_diff
from incremental.diff import diff_snapshots, write_incremental_diff
from incremental.edges_update import upsert_cpg_edges
from incremental.extract_update import upsert_extract_outputs
from incremental.invalidations import InvalidationResult, check_state_store_invalidation
from incremental.nodes_update import upsert_cpg_nodes
from incremental.normalize_update import upsert_normalize_outputs
from incremental.props_update import upsert_cpg_props
from incremental.relspec_update import upsert_relationship_outputs
from incremental.scip_snapshot import (
    build_scip_snapshot,
    diff_scip_snapshots,
    read_scip_snapshot,
    scip_changed_file_ids,
    write_scip_diff,
    write_scip_snapshot,
)
from incremental.snapshot import build_repo_snapshot, read_repo_snapshot, write_repo_snapshot
from incremental.state_store import StateStore
from incremental.types import IncrementalConfig, IncrementalFileChanges
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY


@dataclass(frozen=True)
class CpgDeltaInputs:
    """Delta tables required for CPG props updates."""

    props: TableLike
    nodes: TableLike
    edges: TableLike


@tag(layer="incremental", kind="object")
def incremental_state_store(
    incremental_config: IncrementalConfig,
    repo_root: str,
) -> StateStore | None:
    """Return the incremental state store when enabled.

    Returns
    -------
    StateStore | None
        State store when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    state_dir = incremental_config.state_dir or Path(repo_root) / "build" / "state"
    return StateStore(root=state_dir)


@tag(layer="incremental", kind="object")
def incremental_invalidation(
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
) -> InvalidationResult | None:
    """Validate incremental state signatures and reset on mismatch.

    Returns
    -------
    InvalidationResult | None
        Invalidation outcome, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    return check_state_store_invalidation(
        state_store=incremental_state_store,
        registry=GLOBAL_SCHEMA_REGISTRY,
    )


@tag(layer="incremental", kind="table")
def incremental_repo_snapshot(
    repo_files: TableLike,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Build the repo snapshot when incremental is enabled.

    Returns
    -------
    pa.Table | None
        Snapshot table when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    return build_repo_snapshot(repo_files)


@tag(layer="incremental", kind="table")
def incremental_scip_snapshot(
    scip_documents: TableLike,
    scip_occurrences: TableLike,
    scip_diagnostics: TableLike,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Build the SCIP snapshot when incremental is enabled.

    Returns
    -------
    pa.Table | None
        SCIP snapshot table when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    return build_scip_snapshot(
        scip_documents=scip_documents,
        scip_occurrences=scip_occurrences,
        scip_diagnostics=scip_diagnostics,
    )


@tag(layer="incremental", kind="table")
def incremental_scip_diff(
    incremental_scip_snapshot: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_invalidation: InvalidationResult | None,
) -> pa.Table | None:
    """Compute and persist the SCIP diff for the latest snapshot.

    Returns
    -------
    pa.Table | None
        SCIP diff table when a snapshot and state store are available.
    """
    if incremental_state_store is None or incremental_scip_snapshot is None:
        return None
    if incremental_invalidation is not None and incremental_invalidation.full_refresh:
        prev = None
    else:
        prev = read_scip_snapshot(incremental_state_store)
    diff = diff_scip_snapshots(prev, incremental_scip_snapshot)
    write_scip_snapshot(incremental_state_store, incremental_scip_snapshot)
    write_scip_diff(incremental_state_store, diff)
    return diff


@tag(layer="incremental", kind="object")
def incremental_scip_changed_file_ids(
    incremental_scip_diff: pa.Table | None,
    incremental_repo_snapshot: pa.Table | None,
    incremental_config: IncrementalConfig,
) -> tuple[str, ...]:
    """Return file_ids impacted by SCIP document changes.

    Returns
    -------
    tuple[str, ...]
        File-id values derived from SCIP document changes.
    """
    if not incremental_config.enabled:
        return ()
    return scip_changed_file_ids(incremental_scip_diff, incremental_repo_snapshot)


@tag(layer="incremental", kind="table")
def incremental_diff(
    incremental_repo_snapshot: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_invalidation: InvalidationResult | None,
) -> pa.Table | None:
    """Compute and persist the incremental diff for the latest snapshot.

    Returns
    -------
    pa.Table | None
        Diff table when a snapshot and state store are available.
    """
    if incremental_state_store is None or incremental_repo_snapshot is None:
        return None
    if incremental_invalidation is not None and incremental_invalidation.full_refresh:
        prev = None
    else:
        prev = read_repo_snapshot(incremental_state_store)
    diff = diff_snapshots(prev, incremental_repo_snapshot)
    write_repo_snapshot(incremental_state_store, incremental_repo_snapshot)
    write_incremental_diff(incremental_state_store, diff)
    return diff


@tag(layer="incremental", kind="object")
def incremental_file_changes(
    incremental_diff: pa.Table | None,
    incremental_scip_changed_file_ids: tuple[str, ...],
    incremental_config: IncrementalConfig,
) -> IncrementalFileChanges:
    """Derive incremental file change sets from the latest diffs.

    Returns
    -------
    IncrementalFileChanges
        File-id changes derived from repo + SCIP diffs.
    """
    if not incremental_config.enabled:
        return IncrementalFileChanges()
    changes = file_changes_from_diff(incremental_diff)
    if not incremental_scip_changed_file_ids:
        return changes
    combined = sorted(
        set(changes.changed_file_ids) | set(incremental_scip_changed_file_ids)
    )
    return IncrementalFileChanges(
        changed_file_ids=tuple(combined),
        deleted_file_ids=changes.deleted_file_ids,
        full_refresh=changes.full_refresh,
    )


@tag(layer="incremental", kind="object")
def incremental_extract_updates(
    extract_bundle_tables: Mapping[str, TableLike],
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert extract outputs into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (
        incremental_file_changes.changed_file_ids
        or incremental_file_changes.deleted_file_ids
    ):
        return {}
    return upsert_extract_outputs(
        extract_bundle_tables,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_normalize_updates(
    normalize_outputs_bundle: Mapping[str, TableLike],
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert normalize outputs into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (
        incremental_file_changes.changed_file_ids
        or incremental_file_changes.deleted_file_ids
    ):
        return {}
    return upsert_normalize_outputs(
        normalize_outputs_bundle,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_relationship_updates(
    relationship_output_tables: RelationshipOutputTables,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert relationship outputs into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (
        incremental_file_changes.changed_file_ids
        or incremental_file_changes.deleted_file_ids
    ):
        return {}
    return upsert_relationship_outputs(
        relationship_output_tables.as_dict(),
        state_root=incremental_state_store.root,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_cpg_nodes_updates(
    cpg_nodes_delta: TableLike,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert CPG nodes into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (
        incremental_file_changes.changed_file_ids
        or incremental_file_changes.deleted_file_ids
    ):
        return {}
    return upsert_cpg_nodes(
        cpg_nodes_delta,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_cpg_edges_updates(
    cpg_edges_delta: TableLike,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert CPG edges into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (
        incremental_file_changes.changed_file_ids
        or incremental_file_changes.deleted_file_ids
    ):
        return {}
    return upsert_cpg_edges(
        cpg_edges_delta,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_cpg_delta_inputs(
    cpg_props_delta: TableLike,
    cpg_nodes_delta: TableLike,
    cpg_edges_delta: TableLike,
) -> CpgDeltaInputs:
    """Bundle delta tables required for CPG prop updates.

    Returns
    -------
    CpgDeltaInputs
        Delta tables for CPG props publishing.
    """
    return CpgDeltaInputs(
        props=cpg_props_delta,
        nodes=cpg_nodes_delta,
        edges=cpg_edges_delta,
    )


@tag(layer="incremental", kind="object")
def incremental_cpg_props_updates(
    incremental_cpg_delta_inputs: CpgDeltaInputs,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert CPG props into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (
        incremental_file_changes.changed_file_ids
        or incremental_file_changes.deleted_file_ids
    ):
        return {}
    return upsert_cpg_props(
        incremental_cpg_delta_inputs.props,
        cpg_nodes=incremental_cpg_delta_inputs.nodes,
        cpg_edges=incremental_cpg_delta_inputs.edges,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


__all__ = [
    "incremental_cpg_delta_inputs",
    "incremental_cpg_edges_updates",
    "incremental_cpg_nodes_updates",
    "incremental_cpg_props_updates",
    "incremental_diff",
    "incremental_extract_updates",
    "incremental_file_changes",
    "incremental_invalidation",
    "incremental_normalize_updates",
    "incremental_relationship_updates",
    "incremental_repo_snapshot",
    "incremental_scip_changed_file_ids",
    "incremental_scip_diff",
    "incremental_scip_snapshot",
    "incremental_state_store",
]
