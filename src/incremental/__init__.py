"""Incremental pipeline helpers."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from incremental.changes import file_changes_from_diff
    from incremental.deltas import compute_changed_exports
    from incremental.diff import diff_snapshots, write_incremental_diff
    from incremental.edges_update import upsert_cpg_edges
    from incremental.exports import build_exported_defs_index
    from incremental.exports_update import upsert_exported_defs
    from incremental.extract_update import upsert_extract_outputs
    from incremental.impact import (
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
        InvalidationResult,
        build_invalidation_snapshot,
        check_state_store_invalidation,
        check_state_store_invalidation_with_diff,
        diff_invalidation_snapshots,
        read_invalidation_snapshot,
        reset_state_store,
        rule_plan_diff_table,
        write_invalidation_snapshot,
    )
    from incremental.module_index import build_module_index
    from incremental.nodes_update import upsert_cpg_nodes
    from incremental.normalize_update import upsert_normalize_outputs
    from incremental.props_update import split_props_by_file_id, upsert_cpg_props
    from incremental.publish import publish_cpg_edges, publish_cpg_nodes, publish_cpg_props
    from incremental.relspec_update import (
        impacted_file_ids,
        relspec_inputs_from_state,
        scoped_relspec_resolver,
        upsert_relationship_outputs,
    )
    from incremental.schemas import INCREMENTAL_SCHEMA_REGISTRY
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
    "INCREMENTAL_SCHEMA_REGISTRY",
    "IncrementalConfig",
    "IncrementalFileChanges",
    "IncrementalImpact",
    "InvalidationOutcome",
    "InvalidationResult",
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
    "diff_snapshots",
    "file_changes_from_diff",
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
    "upsert_cpg_edges",
    "upsert_cpg_nodes",
    "upsert_cpg_props",
    "upsert_exported_defs",
    "upsert_extract_outputs",
    "upsert_imports_resolved",
    "upsert_module_index",
    "upsert_normalize_outputs",
    "upsert_relationship_outputs",
    "write_impacted_callers",
    "write_impacted_files",
    "write_impacted_importers",
    "write_incremental_diff",
    "write_invalidation_snapshot",
    "write_repo_snapshot",
    "write_scip_diff",
    "write_scip_fingerprint",
    "write_scip_snapshot",
]

_LAZY_IMPORTS: dict[str, str] = {
    "INCREMENTAL_SCHEMA_REGISTRY": "incremental.schemas",
    "IncrementalConfig": "incremental.types",
    "IncrementalFileChanges": "incremental.types",
    "IncrementalImpact": "incremental.types",
    "InvalidationResult": "incremental.invalidations",
    "StateStore": "incremental.state_store",
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
    "diff_snapshots": "incremental.diff",
    "file_changes_from_diff": "incremental.changes",
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
    "split_props_by_file_id": "incremental.props_update",
    "upsert_cpg_edges": "incremental.edges_update",
    "upsert_cpg_nodes": "incremental.nodes_update",
    "upsert_cpg_props": "incremental.props_update",
    "upsert_exported_defs": "incremental.exports_update",
    "upsert_extract_outputs": "incremental.extract_update",
    "upsert_imports_resolved": "incremental.index_update",
    "upsert_module_index": "incremental.index_update",
    "upsert_normalize_outputs": "incremental.normalize_update",
    "upsert_relationship_outputs": "incremental.relspec_update",
    "write_impacted_callers": "incremental.impact_update",
    "write_impacted_files": "incremental.impact_update",
    "write_impacted_importers": "incremental.impact_update",
    "write_incremental_diff": "incremental.diff",
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
