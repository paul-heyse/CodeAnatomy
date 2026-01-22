"""Incremental pipeline nodes for snapshots, diffs, and updates."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from datafusion import SessionContext
from hamilton.function_modifiers import tag
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.schema import empty_table
from datafusion_engine.registry_bridge import DeltaCdfRegistrationOptions, register_delta_cdf_df
from datafusion_engine.runtime import DataFusionRuntimeProfile, read_delta_as_reader
from hamilton_pipeline.pipeline_types import (
    IncrementalDatasetUpdates,
    IncrementalImpactUpdates,
    RelationshipOutputTables,
    RepoScanConfig,
)
from incremental.cdf_cursors import CdfCursorStore
from incremental.changes import file_changes_from_diff
from incremental.deltas import compute_changed_exports
from incremental.diff import (
    diff_snapshots_with_cdf,
    diff_snapshots_with_delta_cdf,
    write_incremental_diff,
)
from incremental.edges_update import upsert_cpg_edges
from incremental.exports import build_exported_defs_index
from incremental.exports_update import upsert_exported_defs
from incremental.extract_update import upsert_extract_outputs
from incremental.fingerprint_changes import read_dataset_fingerprints
from incremental.impact import (
    ImpactedFileInputs,
    changed_file_impact_table,
    impacted_callers_from_changed_exports,
    impacted_importers_from_changed_exports,
    import_closure_only_from_changed_exports,
    merge_impacted_files,
)
from incremental.impact_update import (
    write_impacted_callers,
    write_impacted_files,
    write_impacted_importers,
)
from incremental.imports_resolved import resolve_imports
from incremental.index_update import upsert_imports_resolved, upsert_module_index
from incremental.invalidations import (
    InvalidationOutcome,
    check_state_store_invalidation_with_diff,
)
from incremental.module_index import build_module_index
from incremental.nodes_update import upsert_cpg_nodes
from incremental.normalize_update import upsert_normalize_outputs
from incremental.props_update import upsert_cpg_props
from incremental.registry_specs import dataset_schema as incremental_dataset_schema
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
from incremental.types import IncrementalConfig, IncrementalFileChanges, IncrementalImpact
from relspec.schema_context import RelspecSchemaContext
from storage.deltalake import DeltaCdfOptions, delta_table_version


@dataclass(frozen=True)
class CpgDeltaInputs:
    """Delta tables required for CPG props updates."""

    props: TableLike
    nodes: TableLike
    edges: TableLike


@dataclass(frozen=True)
class IncrementalImpactBaseInputs:
    """Base inputs for impacted file computation."""

    file_changes: IncrementalFileChanges
    incremental_diff: pa.Table | None
    repo_snapshot: pa.Table | None
    scip_changed_file_ids: tuple[str, ...]


@dataclass(frozen=True)
class IncrementalImpactClosureInputs:
    """Closure inputs for impacted file computation."""

    changed_exports: pa.Table | None
    impacted_callers: pa.Table | None
    impacted_importers: pa.Table | None
    state_store: StateStore | None


def _deregister_table(ctx: SessionContext, name: str | None) -> None:
    if name is None:
        return
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(name)


def _delta_cdf_table(
    *,
    path: Path,
    start_version: int,
    end_version: int,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> tuple[SessionContext, str] | None:
    profile = runtime_profile or DataFusionRuntimeProfile()
    ctx = profile.session_context()
    name = f"__delta_cdf_{uuid.uuid4().hex}"
    try:
        register_delta_cdf_df(
            ctx,
            name=name,
            path=str(path),
            options=DeltaCdfRegistrationOptions(
                cdf_options=DeltaCdfOptions(
                    starting_version=start_version,
                    ending_version=end_version,
                    allow_out_of_range=True,
                ),
                runtime_profile=profile,
            ),
        )
    except (RuntimeError, TypeError, ValueError):
        _deregister_table(ctx, name)
        return None
    else:
        return ctx, name


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
) -> InvalidationOutcome | None:
    """Validate incremental state signatures and reset on mismatch.

    Returns
    -------
    InvalidationOutcome | None
        Invalidation outcome with diff payload, or None when disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    schema_context = RelspecSchemaContext.from_session(DataFusionRuntimeProfile().session_context())
    return check_state_store_invalidation_with_diff(
        state_store=incremental_state_store,
        schema_context=schema_context,
    )


@tag(layer="incremental", kind="table")
def incremental_plan_diff(
    incremental_invalidation: InvalidationOutcome | None,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Return rule plan diff diagnostics when incremental is enabled.

    Returns
    -------
    pa.Table | None
        Diff table when incremental mode is enabled.
    """
    if not incremental_config.enabled or incremental_invalidation is None:
        return None
    return incremental_invalidation.plan_diff


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
def incremental_module_index(
    repo_files_extract: TableLike,
    repo_scan_config: RepoScanConfig,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Build the per-file module index when incremental mode is enabled.

    Returns
    -------
    pa.Table | None
        Module index table when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    return build_module_index(
        repo_files_extract,
        repo_root=Path(repo_scan_config.repo_root),
    )


@tag(layer="incremental", kind="table")
def incremental_imports_resolved(
    cst_imports_norm: TableLike,
    incremental_module_index: pa.Table | None,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Build the resolved imports table when incremental mode is enabled.

    Returns
    -------
    pa.Table | None
        Resolved imports table when incremental mode is enabled.
    """
    if not incremental_config.enabled or incremental_module_index is None:
        return None
    return resolve_imports(cst_imports_norm, incremental_module_index)


@tag(layer="incremental", kind="table")
def incremental_exported_defs(
    cst_defs_norm: TableLike,
    incremental_state_store: StateStore | None,
    ibis_backend: BaseBackend,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Build the exported definitions index when incremental mode is enabled.

    Returns
    -------
    pa.Table | None
        Exported definitions table when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    rel_def_symbol: pa.Table | None = None
    if incremental_state_store is not None:
        rel_def_dir = incremental_state_store.dataset_dir("rel_def_symbol_v1")
        if rel_def_dir.exists():
            rel_def_symbol = read_delta_as_reader(str(rel_def_dir)).read_all()
    return build_exported_defs_index(
        cst_defs_norm,
        backend=ibis_backend,
        rel_def_symbol=rel_def_symbol,
    )


@tag(layer="incremental", kind="table")
def incremental_changed_exports(
    incremental_exported_defs: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    ibis_backend: BaseBackend,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Compute export deltas for changed files.

    Returns
    -------
    pa.Table | None
        Export delta table when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    if incremental_exported_defs is None or incremental_state_store is None:
        return None
    if not incremental_file_changes.changed_file_ids:
        return None
    prev_path = incremental_state_store.dataset_dir("dim_exported_defs_v1")
    prev_exports = str(prev_path) if prev_path.exists() else None
    return compute_changed_exports(
        backend=ibis_backend,
        prev_exports=prev_exports,
        curr_exports=incremental_exported_defs,
        changed_files=_changed_files_table(incremental_file_changes.changed_file_ids),
    )


@tag(layer="incremental", kind="table")
def incremental_impacted_callers(
    incremental_changed_exports: pa.Table | None,
    incremental_state_store: StateStore | None,
    ibis_backend: BaseBackend,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Compute impacted caller files from prior relationship outputs.

    Returns
    -------
    pa.Table | None
        Impacted caller table when incremental mode is enabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if incremental_changed_exports is None:
        return empty_table(incremental_dataset_schema("inc_impacted_callers_v1"))
    rel_qname_dir = incremental_state_store.dataset_dir("rel_callsite_qname_v1")
    rel_symbol_dir = incremental_state_store.dataset_dir("rel_callsite_symbol_v1")
    prev_qname = str(rel_qname_dir) if rel_qname_dir.exists() else None
    prev_symbol = str(rel_symbol_dir) if rel_symbol_dir.exists() else None
    return impacted_callers_from_changed_exports(
        backend=ibis_backend,
        changed_exports=incremental_changed_exports,
        prev_rel_callsite_qname=prev_qname,
        prev_rel_callsite_symbol=prev_symbol,
    )


@tag(layer="incremental", kind="table")
def incremental_impacted_importers(
    incremental_changed_exports: pa.Table | None,
    incremental_state_store: StateStore | None,
    ibis_backend: BaseBackend,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Compute impacted importers from resolved imports.

    Returns
    -------
    pa.Table | None
        Impacted importer table when incremental mode is enabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if incremental_changed_exports is None:
        return empty_table(incremental_dataset_schema("inc_impacted_importers_v1"))
    imports_dir = incremental_state_store.dataset_dir("py_imports_resolved_v1")
    prev_imports = str(imports_dir) if imports_dir.exists() else None
    return impacted_importers_from_changed_exports(
        backend=ibis_backend,
        changed_exports=incremental_changed_exports,
        prev_imports_resolved=prev_imports,
    )


@tag(layer="incremental", kind="object")
def incremental_impact_base_inputs(
    incremental_file_changes: IncrementalFileChanges,
    incremental_diff: pa.Table | None,
    incremental_repo_snapshot: pa.Table | None,
    incremental_scip_changed_file_ids: tuple[str, ...],
) -> IncrementalImpactBaseInputs:
    """Bundle base inputs for impact computation.

    Returns
    -------
    IncrementalImpactBaseInputs
        Base inputs for impacted file derivation.
    """
    return IncrementalImpactBaseInputs(
        file_changes=incremental_file_changes,
        incremental_diff=incremental_diff,
        repo_snapshot=incremental_repo_snapshot,
        scip_changed_file_ids=incremental_scip_changed_file_ids,
    )


@tag(layer="incremental", kind="object")
def incremental_impact_closure_inputs(
    incremental_changed_exports: pa.Table | None,
    incremental_impacted_callers: pa.Table | None,
    incremental_impacted_importers: pa.Table | None,
    incremental_state_store: StateStore | None,
) -> IncrementalImpactClosureInputs:
    """Bundle closure inputs for impact computation.

    Returns
    -------
    IncrementalImpactClosureInputs
        Closure inputs for impacted file derivation.
    """
    return IncrementalImpactClosureInputs(
        changed_exports=incremental_changed_exports,
        impacted_callers=incremental_impacted_callers,
        impacted_importers=incremental_impacted_importers,
        state_store=incremental_state_store,
    )


@tag(layer="incremental", kind="table")
def incremental_impacted_files(
    incremental_impact_base_inputs: IncrementalImpactBaseInputs,
    incremental_impact_closure_inputs: IncrementalImpactClosureInputs,
    ibis_backend: BaseBackend,
    incremental_config: IncrementalConfig,
) -> pa.Table | None:
    """Compute the final impacted file set for incremental runs.

    Returns
    -------
    pa.Table | None
        Impacted file diagnostics table when incremental mode is enabled.
    """
    if not incremental_config.enabled:
        return None
    changed_table = changed_file_impact_table(
        file_changes=incremental_impact_base_inputs.file_changes,
        incremental_diff=incremental_impact_base_inputs.incremental_diff,
        repo_snapshot=incremental_impact_base_inputs.repo_snapshot,
        scip_changed_file_ids=incremental_impact_base_inputs.scip_changed_file_ids,
    )
    import_closure_only: pa.Table | None = None
    if (
        incremental_impact_closure_inputs.state_store is not None
        and incremental_impact_closure_inputs.changed_exports is not None
        and incremental_config.impact_strategy in {"import_closure", "hybrid"}
    ):
        imports_dir = incremental_impact_closure_inputs.state_store.dataset_dir(
            "py_imports_resolved_v1"
        )
        prev_imports = str(imports_dir) if imports_dir.exists() else None
        import_closure_only = import_closure_only_from_changed_exports(
            backend=ibis_backend,
            changed_exports=incremental_impact_closure_inputs.changed_exports,
            prev_imports_resolved=prev_imports,
        )
    return merge_impacted_files(
        ibis_backend,
        ImpactedFileInputs(
            changed_files=changed_table,
            callers=incremental_impact_closure_inputs.impacted_callers,
            importers=incremental_impact_closure_inputs.impacted_importers,
            import_closure_only=import_closure_only,
        ),
        strategy=incremental_config.impact_strategy,
    )


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
    incremental_invalidation: InvalidationOutcome | None,
) -> pa.Table | None:
    """Compute and persist the SCIP diff for the latest snapshot.

    Returns
    -------
    pa.Table | None
        SCIP diff table when a snapshot and state store are available.
    """
    if incremental_state_store is None or incremental_scip_snapshot is None:
        return None
    if incremental_invalidation is not None and incremental_invalidation.result.full_refresh:
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
    incremental_invalidation: InvalidationOutcome | None,
    ctx: ExecutionContext | None = None,
) -> pa.Table | None:
    """Compute and persist the incremental diff for the latest snapshot.

    Returns
    -------
    pa.Table | None
        Diff table when a snapshot and state store are available.
    """
    if incremental_state_store is None or incremental_repo_snapshot is None:
        return None

    snapshot_path = incremental_state_store.repo_snapshot_path()
    prev: pa.Table | None
    prev_version: int | None = None

    # Check if we need a full refresh
    if incremental_invalidation is not None and incremental_invalidation.result.full_refresh:
        prev = None
    else:
        prev = read_repo_snapshot(incremental_state_store)
        if prev is not None:
            prev_version = delta_table_version(str(snapshot_path))

    # Write the new snapshot
    write_result = write_repo_snapshot(incremental_state_store, incremental_repo_snapshot)

    # Try Delta CDF-driven incremental diff first (primary path)
    if prev is not None and prev_version is not None and write_result.version is not None:
        start_version = prev_version + 1
        if start_version <= write_result.version:
            # Try to use CDF table registration
            cdf_ref = _delta_cdf_table(
                path=snapshot_path,
                start_version=start_version,
                end_version=write_result.version,
                runtime_profile=ctx.runtime.datafusion if ctx is not None else None,
            )
            if cdf_ref is not None:
                cdf_ctx, cdf_name = cdf_ref
                try:
                    diff = diff_snapshots_with_cdf(
                        prev,
                        incremental_repo_snapshot,
                        ctx=cdf_ctx,
                        cdf_table=cdf_name,
                    )
                    write_incremental_diff(incremental_state_store, diff)
                    return diff
                finally:
                    _deregister_table(cdf_ctx, cdf_name)

    # Fallback: Use cursor-based Delta CDF if available
    cursor_store = CdfCursorStore(cursors_path=incremental_state_store.cdf_cursors_path())
    cdf_result = diff_snapshots_with_delta_cdf(
        dataset_path=str(snapshot_path),
        cursor_store=cursor_store,
        dataset_name="repo_snapshot",
        filter_policy=None,
    )

    # If CDF returns a table, we can use it with diff_snapshots_with_cdf
    if cdf_result is not None and prev is not None:
        profile = DataFusionRuntimeProfile()
        cdf_ctx = profile.session_context()
        cdf_name = f"__cdf_fallback_{uuid.uuid4().hex}"
        try:
            cdf_ctx.register_record_batches(cdf_name, [list(cdf_result.to_batches())])
            diff = diff_snapshots_with_cdf(
                prev,
                incremental_repo_snapshot,
                ctx=cdf_ctx,
                cdf_table=cdf_name,
            )
            write_incremental_diff(incremental_state_store, diff)
            return diff
        finally:
            _deregister_table(cdf_ctx, cdf_name)

    # Final fallback: Full snapshot comparison (for first run or when CDF unavailable)
    # Build a minimal diff table for the "added" case (all files in current snapshot)
    profile = DataFusionRuntimeProfile()
    cdf_ctx = profile.session_context()
    sql_options = profile.sql_options()
    cur_name = f"__snapshot_{uuid.uuid4().hex}"
    try:
        cdf_ctx.register_record_batches(cur_name, [list(incremental_repo_snapshot.to_batches())])
        sql = f"""
        SELECT
          cur.file_id AS file_id,
          cur.path AS path,
          'added' AS change_kind,
          CAST(NULL AS STRING) AS prev_path,
          cur.path AS cur_path,
          CAST(NULL AS STRING) AS prev_file_sha256,
          cur.file_sha256 AS cur_file_sha256,
          CAST(NULL AS BIGINT) AS prev_size_bytes,
          cur.size_bytes AS cur_size_bytes,
          CAST(NULL AS BIGINT) AS prev_mtime_ns,
          cur.mtime_ns AS cur_mtime_ns
        FROM {cur_name} AS cur
        """
        diff = cdf_ctx.sql_with_options(sql, sql_options).to_arrow_table()
        write_incremental_diff(incremental_state_store, diff)
        return diff
    finally:
        _deregister_table(cdf_ctx, cur_name)


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
    combined = sorted(set(changes.changed_file_ids) | set(incremental_scip_changed_file_ids))
    return IncrementalFileChanges(
        changed_file_ids=tuple(combined),
        deleted_file_ids=changes.deleted_file_ids,
        full_refresh=changes.full_refresh,
    )


def _changed_files_table(
    changed_file_ids: tuple[str, ...],
) -> pa.Table:
    schema = pa.schema([("file_id", pa.string())])
    if not changed_file_ids:
        return table_from_arrays(schema, columns={}, num_rows=0)
    return table_from_arrays(
        schema,
        columns={"file_id": pa.array(list(changed_file_ids), type=pa.string())},
        num_rows=len(changed_file_ids),
    )


def _table_file_ids(table: pa.Table) -> tuple[str, ...]:
    if "file_id" not in table.column_names:
        return ()
    values = table["file_id"]
    cleaned = [value.as_py() if isinstance(value, pa.Scalar) else value for value in values]
    cleaned = [value for value in cleaned if isinstance(value, str) and value]
    return tuple(sorted(set(cleaned)))


@tag(layer="incremental", kind="object")
def incremental_module_index_updates(
    incremental_module_index: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert module index outputs into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
        return {}
    if incremental_module_index is None:
        return {}
    return upsert_module_index(
        incremental_module_index,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_imports_resolved_updates(
    incremental_imports_resolved: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
    incremental_module_index_updates: Mapping[str, str] | None,
) -> Mapping[str, str] | None:
    """Upsert resolved imports outputs into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    _ = incremental_module_index_updates
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
        return {}
    if incremental_imports_resolved is None:
        return {}
    return upsert_imports_resolved(
        incremental_imports_resolved,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_exported_defs_updates(
    incremental_exported_defs: pa.Table | None,
    incremental_changed_exports: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Upsert exported definitions into the incremental state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    _ = incremental_changed_exports
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
        return {}
    if incremental_exported_defs is None:
        return {}
    return upsert_exported_defs(
        incremental_exported_defs,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_dataset_updates(
    incremental_extract_updates: Mapping[str, str] | None,
    incremental_normalize_updates: Mapping[str, str] | None,
    incremental_module_index_updates: Mapping[str, str] | None,
    incremental_imports_resolved_updates: Mapping[str, str] | None,
    incremental_exported_defs_updates: Mapping[str, str] | None,
) -> IncrementalDatasetUpdates:
    """Bundle dataset update paths for incremental gating.

    Returns
    -------
    IncrementalDatasetUpdates
        Dataset update paths grouped for gating.
    """
    return IncrementalDatasetUpdates(
        extract_updates=incremental_extract_updates,
        normalize_updates=incremental_normalize_updates,
        module_index_updates=incremental_module_index_updates,
        imports_resolved_updates=incremental_imports_resolved_updates,
        exported_defs_updates=incremental_exported_defs_updates,
    )


@tag(layer="incremental", kind="object")
def incremental_impacted_callers_updates(
    incremental_impacted_callers: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Persist impacted caller diagnostics to the state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if incremental_impacted_callers is None:
        return {}
    return write_impacted_callers(
        incremental_impacted_callers,
        state_store=incremental_state_store,
    )


@tag(layer="incremental", kind="object")
def incremental_impacted_importers_updates(
    incremental_impacted_importers: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Persist impacted importer diagnostics to the state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if incremental_impacted_importers is None:
        return {}
    return write_impacted_importers(
        incremental_impacted_importers,
        state_store=incremental_state_store,
    )


@tag(layer="incremental", kind="object")
def incremental_impacted_files_updates(
    incremental_impacted_files: pa.Table | None,
    incremental_state_store: StateStore | None,
    incremental_config: IncrementalConfig,
) -> Mapping[str, str] | None:
    """Persist impacted file diagnostics to the state store.

    Returns
    -------
    Mapping[str, str] | None
        Updated dataset paths, or None when incremental is disabled.
    """
    if not incremental_config.enabled or incremental_state_store is None:
        return None
    if incremental_impacted_files is None:
        return {}
    return write_impacted_files(
        incremental_impacted_files,
        state_store=incremental_state_store,
    )


@tag(layer="incremental", kind="object")
def incremental_impact_updates(
    incremental_impacted_callers_updates: Mapping[str, str] | None,
    incremental_impacted_importers_updates: Mapping[str, str] | None,
    incremental_impacted_files_updates: Mapping[str, str] | None,
) -> IncrementalImpactUpdates:
    """Bundle impact update paths for incremental gating.

    Returns
    -------
    IncrementalImpactUpdates
        Impact update paths grouped for gating.
    """
    return IncrementalImpactUpdates(
        impacted_callers_updates=incremental_impacted_callers_updates,
        impacted_importers_updates=incremental_impacted_importers_updates,
        impacted_files_updates=incremental_impacted_files_updates,
    )


@tag(layer="incremental", kind="object")
def incremental_impact(
    incremental_file_changes: IncrementalFileChanges,
    incremental_impacted_files: pa.Table | None,
    incremental_config: IncrementalConfig,
) -> IncrementalImpact:
    """Return the current incremental impact scope.

    Returns
    -------
    IncrementalImpact
        Impact scope derived from incremental diffs.
    """
    if not incremental_config.enabled:
        return IncrementalImpact()
    impacted = tuple(sorted(set(incremental_file_changes.changed_file_ids)))
    if incremental_impacted_files is not None:
        impacted = _table_file_ids(incremental_impacted_files)
    return IncrementalImpact(
        changed_file_ids=incremental_file_changes.changed_file_ids,
        deleted_file_ids=incremental_file_changes.deleted_file_ids,
        impacted_file_ids=impacted,
        full_refresh=incremental_file_changes.full_refresh,
    )


@tag(layer="incremental", kind="object")
def incremental_extract_impact(
    incremental_file_changes: IncrementalFileChanges,
    incremental_config: IncrementalConfig,
) -> IncrementalImpact:
    """Return impact scope for extraction inputs.

    Returns
    -------
    IncrementalImpact
        Impact scope derived from repo diffs for extraction.
    """
    if not incremental_config.enabled:
        return IncrementalImpact()
    impacted = tuple(sorted(set(incremental_file_changes.changed_file_ids)))
    return IncrementalImpact(
        changed_file_ids=incremental_file_changes.changed_file_ids,
        deleted_file_ids=incremental_file_changes.deleted_file_ids,
        impacted_file_ids=impacted,
        full_refresh=incremental_file_changes.full_refresh,
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
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
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
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
        return {}
    return upsert_normalize_outputs(
        normalize_outputs_bundle,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


@tag(layer="incremental", kind="object")
def incremental_relationship_updates(
    relationship_output_tables: RelationshipOutputTables,
    relationship_output_fingerprints_map: Mapping[str, str] | None,
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
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
        return {}
    outputs = relationship_output_tables.as_dict()
    if relationship_output_fingerprints_map and not incremental_file_changes.full_refresh:
        previous = read_dataset_fingerprints(incremental_state_store)
        unchanged = {
            name
            for name, fingerprint in relationship_output_fingerprints_map.items()
            if fingerprint and previous.get(name) == fingerprint
        }
        if unchanged:
            outputs = {name: table for name, table in outputs.items() if name not in unchanged}
            if not outputs:
                return {}
    return upsert_relationship_outputs(
        outputs,
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
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
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
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
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
    if not (incremental_file_changes.changed_file_ids or incremental_file_changes.deleted_file_ids):
        return {}
    return upsert_cpg_props(
        incremental_cpg_delta_inputs.props,
        cpg_nodes=incremental_cpg_delta_inputs.nodes,
        cpg_edges=incremental_cpg_delta_inputs.edges,
        state_store=incremental_state_store,
        changes=incremental_file_changes,
    )


__all__ = [
    "incremental_changed_exports",
    "incremental_cpg_delta_inputs",
    "incremental_cpg_edges_updates",
    "incremental_cpg_nodes_updates",
    "incremental_cpg_props_updates",
    "incremental_diff",
    "incremental_exported_defs",
    "incremental_exported_defs_updates",
    "incremental_extract_impact",
    "incremental_extract_updates",
    "incremental_file_changes",
    "incremental_impact",
    "incremental_imports_resolved",
    "incremental_imports_resolved_updates",
    "incremental_invalidation",
    "incremental_module_index",
    "incremental_module_index_updates",
    "incremental_normalize_updates",
    "incremental_plan_diff",
    "incremental_relationship_updates",
    "incremental_repo_snapshot",
    "incremental_scip_changed_file_ids",
    "incremental_scip_diff",
    "incremental_scip_snapshot",
    "incremental_state_store",
]
