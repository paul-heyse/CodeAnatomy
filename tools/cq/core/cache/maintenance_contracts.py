"""Contracts for CQ cache maintenance snapshots."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class CacheMaintenanceSnapshotV1(CqStruct, frozen=True):
    """Snapshot counters emitted by cache maintenance operations."""

    hits: int = 0
    misses: int = 0
    expired_removed: int = 0
    culled_removed: int = 0
    integrity_errors: int = 0


__all__ = ["CacheMaintenanceSnapshotV1"]
