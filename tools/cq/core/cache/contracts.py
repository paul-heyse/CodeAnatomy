"""Typed cache payload contracts for CQ runtime cache namespaces.

For lightweight contracts without heavy dependencies, see base_contracts.py.
This module contains search-related contracts that depend on heavy modules
like search and tree-sitter.
"""

from __future__ import annotations

import msgspec

from tools.cq.core.cache.base_contracts import (
    CacheMaintenanceSnapshotV1,
    CacheRuntimeTuningV1,
    LaneCoordinationPolicyV1,
    TreeSitterBlobRefV1,
    TreeSitterCacheEnvelopeV1,
)
from tools.cq.core.contracts_constraints import NonNegativeInt
from tools.cq.core.structs import CqCacheStruct


class ScopeFileStatCacheV1(CqCacheStruct, frozen=True):
    """Stable file stat tuple persisted in scope snapshot artifacts."""

    path: str
    size_bytes: NonNegativeInt = 0
    mtime_ns: NonNegativeInt = 0


class ScopeSnapshotCacheV1(CqCacheStruct, frozen=True):
    """Cached scope snapshot fingerprint payload."""

    language: str
    scope_globs: tuple[str, ...] = ()
    scope_roots: tuple[str, ...] = ()
    inventory_token: dict[str, object] = msgspec.field(default_factory=dict)
    files: list[ScopeFileStatCacheV1] = msgspec.field(default_factory=list)
    digest: str = ""


class CallsTargetCacheV1(CqCacheStruct, frozen=True):
    """Cached calls-target metadata payload."""

    target_location: tuple[str, int] | None = None
    target_callees: dict[str, int] = msgspec.field(default_factory=dict)
    snapshot_digest: str | None = None


class SearchArtifactBundleV1(CqCacheStruct, frozen=True):
    """Cache-backed object-resolved search artifact bundle."""

    run_id: str
    query: str
    macro: str = "search"
    summary: dict[str, object] = msgspec.field(default_factory=dict)
    object_summaries: list[dict[str, object]] = msgspec.field(default_factory=list)
    occurrences: list[dict[str, object]] = msgspec.field(default_factory=list)
    diagnostics: dict[str, object] = msgspec.field(default_factory=dict)
    snippets: dict[str, str] = msgspec.field(default_factory=dict)
    created_ms: float = 0.0


class SearchArtifactIndexEntryV1(CqCacheStruct, frozen=True):
    """Index row for one cache-backed search artifact bundle."""

    run_id: str
    cache_key: str
    query: str
    macro: str = "search"
    created_ms: float = 0.0


class SearchArtifactIndexV1(CqCacheStruct, frozen=True):
    """Run-scoped index of cache-backed search artifact bundles."""

    entries: list[SearchArtifactIndexEntryV1] = msgspec.field(default_factory=list)


__all__ = [
    "CacheMaintenanceSnapshotV1",
    "CacheRuntimeTuningV1",
    "CallsTargetCacheV1",
    "LaneCoordinationPolicyV1",
    "ScopeFileStatCacheV1",
    "ScopeSnapshotCacheV1",
    "SearchArtifactBundleV1",
    "SearchArtifactIndexEntryV1",
    "SearchArtifactIndexV1",
    "TreeSitterBlobRefV1",
    "TreeSitterCacheEnvelopeV1",
]
