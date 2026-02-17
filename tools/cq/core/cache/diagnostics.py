"""Cache backend diagnostics helpers."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.telemetry import snapshot_cache_telemetry
from tools.cq.core.runtime.env_namespace import env_bool


def _namespace_metrics_payload() -> dict[str, dict[str, object]]:
    """Build payload data for namespace metrics snapshots.

    Returns:
        dict[str, dict[str, object]]: Namespace metrics grouped by cache namespace.
    """
    metrics = snapshot_cache_telemetry()
    payload: dict[str, dict[str, object]] = {}
    for namespace, counters in sorted(metrics.items()):
        gets = int(counters.gets)
        hits = int(counters.hits)
        misses = int(counters.misses)
        hit_ratio = (hits / gets) if gets > 0 else None
        miss_ratio = (misses / gets) if gets > 0 else None
        payload[namespace] = {
            "gets": gets,
            "hits": hits,
            "misses": misses,
            "hit_ratio": hit_ratio,
            "miss_ratio": miss_ratio,
            "sets": int(counters.sets),
            "set_failures": int(counters.set_failures),
            "deletes": int(counters.deletes),
            "delete_failures": int(counters.delete_failures),
            "evictions": int(counters.evictions),
            "eviction_failures": int(counters.eviction_failures),
            "decode_failures": int(counters.decode_failures),
            "key_cardinality": int(counters.key_cardinality),
            "key_size_le_64": int(counters.key_size_le_64),
            "key_size_le_128": int(counters.key_size_le_128),
            "key_size_le_256": int(counters.key_size_le_256),
            "key_size_gt_256": int(counters.key_size_gt_256),
            "timeouts": int(counters.timeouts),
            "aborts": int(counters.aborts),
            "last_volume_bytes": int(counters.last_volume_bytes),
            "cull_calls": int(counters.cull_calls),
            "cull_removed": int(counters.cull_removed),
        }
    return payload


def snapshot_backend_metrics(*, root: Path) -> dict[str, object]:
    """Collect backend cache metrics for summary/diagnostics payloads.

    Returns:
        dict[str, object]: Backend stats, volume, and namespace-level payload.
    """
    backend = get_cq_cache_backend(root=root)
    payload: dict[str, object] = {}
    stats = backend.stats()
    if stats:
        payload["stats"] = dict(stats)
    volume = backend.volume()
    if isinstance(volume, int) and volume >= 0:
        payload["volume_bytes"] = volume
    if env_bool(os.getenv("CQ_CACHE_CULL_ON_SNAPSHOT"), default=False):
        removed = backend.cull()
        if isinstance(removed, int):
            payload["cull_removed"] = max(0, removed)
    policy = default_cache_policy(root=root)
    payload["stats_enabled"] = bool(policy.statistics_enabled)
    payload["size_limit_bytes"] = int(policy.size_limit_bytes)
    payload["cull_limit"] = int(policy.cull_limit)
    payload["eviction_policy"] = str(policy.eviction_policy)
    namespace_metrics = _namespace_metrics_payload()
    if namespace_metrics:
        payload["namespaces"] = namespace_metrics
    return payload


__all__ = ["snapshot_backend_metrics"]
