"""Incremental pipeline helpers."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from incremental.changes import file_changes_from_cdf
    from incremental.delta_updates import (
        OverwriteDatasetSpec,
        PartitionedDatasetSpec,
        upsert_cpg_edges,
        upsert_cpg_nodes,
        upsert_exported_defs,
        upsert_extract_outputs,
        upsert_imports_resolved,
        upsert_module_index,
        upsert_normalize_outputs,
        upsert_partitioned_dataset,
    )
    from incremental.deltas import compute_changed_exports
    from incremental.diff import diff_snapshots_with_delta_cdf, write_incremental_diff
    from incremental.exports import build_exported_defs_index
    from incremental.impact import (
        changed_file_impact_table,
        impacted_callers_from_changed_exports,
        impacted_importers_from_changed_exports,
        import_closure_only_from_changed_exports,
        merge_impacted_files,
    )
    from incremental.imports_resolved import resolve_imports
    from incremental.invalidations import (
        InvalidationOutcome,
        InvalidationResult,
        build_invalidation_snapshot,
        check_state_store_invalidation,
        check_state_store_invalidation_with_diff,
        diff_invalidation_snapshots,
        read_invalidation_snapshot,
        reset_state_store,
        rule_plan_diff_table,
        update_invalidation_snapshot,
        write_invalidation_snapshot,
    )
    from incremental.metadata import (
        write_cdf_cursor_snapshot,
        write_incremental_artifacts,
        write_incremental_metadata,
    )
    from incremental.module_index import build_module_index
    from incremental.props import split_props_by_file_id, upsert_cpg_props
    from incremental.publish import publish_cpg_edges, publish_cpg_nodes, publish_cpg_props
    from incremental.relspec_update import (
        impacted_file_ids,
        relspec_inputs_from_state,
        scoped_relspec_resolver,
        upsert_relationship_outputs,
    )
    from incremental.runtime import IncrementalRuntime
    from incremental.scip_fingerprint import (
        read_scip_fingerprint,
        scip_fingerprint_changed,
        scip_index_fingerprint,
        write_scip_fingerprint,
    )
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

__all__ = [
    "IncrementalConfig",
    "IncrementalFileChanges",
    "IncrementalImpact",
    "IncrementalRuntime",
    "InvalidationOutcome",
    "InvalidationResult",
    "OverwriteDatasetSpec",
    "PartitionedDatasetSpec",
    "StateStore",
    "build_exported_defs_index",
    "build_invalidation_snapshot",
    "build_module_index",
    "build_repo_snapshot",
    "build_scip_snapshot",
    "changed_file_impact_table",
    "check_state_store_invalidation",
    "check_state_store_invalidation_with_diff",
    "compute_changed_exports",
    "diff_invalidation_snapshots",
    "diff_scip_snapshots",
    "diff_snapshots_with_delta_cdf",
    "file_changes_from_cdf",
    "impacted_callers_from_changed_exports",
    "impacted_file_ids",
    "impacted_importers_from_changed_exports",
    "import_closure_only_from_changed_exports",
    "merge_impacted_files",
    "publish_cpg_edges",
    "publish_cpg_nodes",
    "publish_cpg_props",
    "read_invalidation_snapshot",
    "read_repo_snapshot",
    "read_scip_fingerprint",
    "read_scip_snapshot",
    "relspec_inputs_from_state",
    "reset_state_store",
    "resolve_imports",
    "rule_plan_diff_table",
    "scip_changed_file_ids",
    "scip_fingerprint_changed",
    "scip_index_fingerprint",
    "scoped_relspec_resolver",
    "split_props_by_file_id",
    "update_invalidation_snapshot",
    "upsert_cpg_edges",
    "upsert_cpg_nodes",
    "upsert_cpg_props",
    "upsert_exported_defs",
    "upsert_extract_outputs",
    "upsert_imports_resolved",
    "upsert_module_index",
    "upsert_normalize_outputs",
    "upsert_partitioned_dataset",
    "upsert_relationship_outputs",
    "write_cdf_cursor_snapshot",
    "write_incremental_artifacts",
    "write_incremental_diff",
    "write_incremental_metadata",
    "write_invalidation_snapshot",
    "write_repo_snapshot",
    "write_scip_diff",
    "write_scip_fingerprint",
    "write_scip_snapshot",
]

_LAZY_IMPORTS: dict[str, str] = {
    "IncrementalConfig": "incremental.types",
    "IncrementalFileChanges": "incremental.types",
    "IncrementalImpact": "incremental.types",
    "IncrementalRuntime": "incremental.runtime",
    "InvalidationResult": "incremental.invalidations",
    "OverwriteDatasetSpec": "incremental.delta_updates",
    "StateStore": "incremental.state_store",
    "PartitionedDatasetSpec": "incremental.delta_updates",
    "build_exported_defs_index": "incremental.exports",
    "build_invalidation_snapshot": "incremental.invalidations",
    "build_module_index": "incremental.module_index",
    "build_repo_snapshot": "incremental.snapshot",
    "build_scip_snapshot": "incremental.scip_snapshot",
    "changed_file_impact_table": "incremental.impact",
    "check_state_store_invalidation": "incremental.invalidations",
    "compute_changed_exports": "incremental.deltas",
    "diff_invalidation_snapshots": "incremental.invalidations",
    "diff_scip_snapshots": "incremental.scip_snapshot",
    "diff_snapshots_with_delta_cdf": "incremental.diff",
    "file_changes_from_cdf": "incremental.changes",
    "impacted_callers_from_changed_exports": "incremental.impact",
    "impacted_file_ids": "incremental.relspec_update",
    "impacted_importers_from_changed_exports": "incremental.impact",
    "import_closure_only_from_changed_exports": "incremental.impact",
    "merge_impacted_files": "incremental.impact",
    "publish_cpg_edges": "incremental.publish",
    "publish_cpg_nodes": "incremental.publish",
    "publish_cpg_props": "incremental.publish",
    "read_invalidation_snapshot": "incremental.invalidations",
    "read_repo_snapshot": "incremental.snapshot",
    "read_scip_fingerprint": "incremental.scip_fingerprint",
    "read_scip_snapshot": "incremental.scip_snapshot",
    "relspec_inputs_from_state": "incremental.relspec_update",
    "reset_state_store": "incremental.invalidations",
    "resolve_imports": "incremental.imports_resolved",
    "scip_changed_file_ids": "incremental.scip_snapshot",
    "scip_fingerprint_changed": "incremental.scip_fingerprint",
    "scip_index_fingerprint": "incremental.scip_fingerprint",
    "scoped_relspec_resolver": "incremental.relspec_update",
    "split_props_by_file_id": "incremental.props",
    "upsert_cpg_edges": "incremental.delta_updates",
    "upsert_cpg_nodes": "incremental.delta_updates",
    "upsert_cpg_props": "incremental.props",
    "upsert_exported_defs": "incremental.delta_updates",
    "upsert_extract_outputs": "incremental.delta_updates",
    "upsert_imports_resolved": "incremental.delta_updates",
    "upsert_module_index": "incremental.delta_updates",
    "upsert_normalize_outputs": "incremental.delta_updates",
    "upsert_partitioned_dataset": "incremental.delta_updates",
    "upsert_relationship_outputs": "incremental.relspec_update",
    "write_cdf_cursor_snapshot": "incremental.metadata",
    "write_incremental_diff": "incremental.diff",
    "write_incremental_artifacts": "incremental.metadata",
    "write_incremental_metadata": "incremental.metadata",
    "update_invalidation_snapshot": "incremental.invalidations",
    "write_invalidation_snapshot": "incremental.invalidations",
    "write_repo_snapshot": "incremental.snapshot",
    "write_scip_diff": "incremental.scip_snapshot",
    "write_scip_fingerprint": "incremental.scip_fingerprint",
    "write_scip_snapshot": "incremental.scip_snapshot",
}


def __getattr__(name: str) -> object:
    module_path = _LAZY_IMPORTS.get(name)
    if module_path is None:
        msg = f"module 'incremental' has no attribute {name!r}"
        raise AttributeError(msg)
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
