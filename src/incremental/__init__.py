"""Incremental pipeline helpers."""

from incremental.changes import file_changes_from_diff
from incremental.diff import diff_snapshots, write_incremental_diff
from incremental.edges_update import upsert_cpg_edges
from incremental.extract_update import upsert_extract_outputs
from incremental.invalidations import (
    InvalidationResult,
    build_invalidation_snapshot,
    check_state_store_invalidation,
    diff_invalidation_snapshots,
    read_invalidation_snapshot,
    reset_state_store,
    write_invalidation_snapshot,
)
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
from incremental.snapshot import (
    build_repo_snapshot,
    read_repo_snapshot,
    write_repo_snapshot,
)
from incremental.state_store import StateStore
from incremental.types import IncrementalConfig, IncrementalFileChanges

__all__ = [
    "IncrementalConfig",
    "IncrementalFileChanges",
    "InvalidationResult",
    "StateStore",
    "build_invalidation_snapshot",
    "build_repo_snapshot",
    "build_scip_snapshot",
    "check_state_store_invalidation",
    "diff_invalidation_snapshots",
    "diff_scip_snapshots",
    "diff_snapshots",
    "file_changes_from_diff",
    "impacted_file_ids",
    "publish_cpg_edges",
    "publish_cpg_nodes",
    "publish_cpg_props",
    "read_invalidation_snapshot",
    "read_repo_snapshot",
    "read_scip_fingerprint",
    "read_scip_snapshot",
    "relspec_inputs_from_state",
    "reset_state_store",
    "scip_changed_file_ids",
    "scip_fingerprint_changed",
    "scip_index_fingerprint",
    "scoped_relspec_resolver",
    "split_props_by_file_id",
    "upsert_cpg_edges",
    "upsert_cpg_nodes",
    "upsert_cpg_props",
    "upsert_extract_outputs",
    "upsert_normalize_outputs",
    "upsert_relationship_outputs",
    "write_incremental_diff",
    "write_invalidation_snapshot",
    "write_repo_snapshot",
    "write_scip_diff",
    "write_scip_fingerprint",
    "write_scip_snapshot",
]
