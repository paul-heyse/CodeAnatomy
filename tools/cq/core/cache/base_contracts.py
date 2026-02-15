"""Base cache contracts without heavy dependencies.

This module contains cache contract definitions that don't depend on heavy
modules like search or tree-sitter, avoiding circular import issues. These
contracts can be imported by core cache modules like cache_runtime_tuning,
maintenance, etc.

For search-related contracts (SearchArtifactBundleV1, etc.), see contracts.py.
"""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct

# Tree-Sitter Blob Store Contracts


class TreeSitterBlobRefV1(CqStruct, frozen=True):
    """Reference metadata for one persisted blob payload."""

    blob_id: str
    storage_key: str
    size_bytes: int
    path: str | None = None


# Coordination Contracts


class LaneCoordinationPolicyV1(CqStruct, frozen=True):
    """Coordination policy for bounded tree-sitter lane execution."""

    semaphore_key: str = "cq:tree_sitter:lanes"
    lock_key_suffix: str = ":lock"
    reentrant_key_suffix: str = ":reentrant"
    lane_limit: int = 4
    ttl_seconds: int = 15


# Maintenance Contracts


class CacheMaintenanceSnapshotV1(CqStruct, frozen=True):
    """Snapshot counters emitted by cache maintenance operations."""

    hits: int = 0
    misses: int = 0
    expired_removed: int = 0
    culled_removed: int = 0
    integrity_errors: int = 0


# Runtime Tuning Contracts


class CacheRuntimeTuningV1(CqStruct, frozen=True):
    """Resolved runtime tuning parameters for cache backend operations."""

    cull_limit: int = 16
    eviction_policy: str = "least-recently-stored"
    statistics_enabled: bool = False
    create_tag_index: bool = True
    sqlite_mmap_size: int = 0
    sqlite_cache_size: int = 0
    transaction_batch_size: int = 128


# Tree-Sitter Cache Store Contracts


class TreeSitterCacheEnvelopeV1(CqStruct, frozen=True):
    """Canonical cache envelope for tree-sitter enrichment payloads."""

    language: str
    file_hash: str
    grammar_hash: str = ""
    query_pack_hash: str = ""
    scope_hash: str = ""
    payload: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = [
    "CacheMaintenanceSnapshotV1",
    "CacheRuntimeTuningV1",
    "LaneCoordinationPolicyV1",
    "TreeSitterBlobRefV1",
    "TreeSitterCacheEnvelopeV1",
]
