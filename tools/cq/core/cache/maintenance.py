"""Diskcache maintenance helpers for CQ cache backends."""

from __future__ import annotations

from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.cache.maintenance_contracts import CacheMaintenanceSnapshotV1


def _to_int(value: object) -> int:
    if isinstance(value, int):
        return int(value)
    return 0


def maintenance_tick(backend: CqCacheBackend) -> CacheMaintenanceSnapshotV1:
    """Run lightweight maintenance calls against disk-backed cache when available.

    Returns:
        CacheMaintenanceSnapshotV1: A snapshot of cache maintenance counters and
            outcomes.
    """
    stats = backend.stats()
    raw_hits: object = stats.get("hits", 0) if isinstance(stats, dict) else 0
    raw_misses: object = stats.get("misses", 0) if isinstance(stats, dict) else 0
    hits = _to_int(raw_hits)
    misses = _to_int(raw_misses)

    expired_raw = backend.expire()
    expired_removed = int(expired_raw) if isinstance(expired_raw, int) else 0

    culled_raw = backend.cull()
    culled_removed = int(culled_raw) if isinstance(culled_raw, int) else 0

    _ = backend.volume()

    integrity_raw = backend.check(fix=False)
    integrity_errors = int(integrity_raw) if isinstance(integrity_raw, int) else 0
    if integrity_errors > 0:
        _ = backend.check(fix=True)
    return CacheMaintenanceSnapshotV1(
        hits=int(hits),
        misses=int(misses),
        expired_removed=expired_removed,
        culled_removed=culled_removed,
        integrity_errors=integrity_errors,
    )


__all__ = ["maintenance_tick"]
