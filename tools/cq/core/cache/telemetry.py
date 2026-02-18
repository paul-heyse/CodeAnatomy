"""Process-local cache telemetry counters for CQ namespaces."""

from __future__ import annotations

import threading

from tools.cq.core.cache.telemetry_store import CacheTelemetryStore
from tools.cq.core.structs import CqStruct


class CacheNamespaceTelemetry(CqStruct, frozen=True):
    """Snapshot of telemetry counters for one cache namespace."""

    gets: int = 0
    hits: int = 0
    misses: int = 0
    sets: int = 0
    set_failures: int = 0
    deletes: int = 0
    delete_failures: int = 0
    evictions: int = 0
    eviction_failures: int = 0
    decode_failures: int = 0
    key_cardinality: int = 0
    key_size_le_64: int = 0
    key_size_le_128: int = 0
    key_size_le_256: int = 0
    key_size_gt_256: int = 0
    timeouts: int = 0
    aborts: int = 0
    last_volume_bytes: int = 0
    cull_calls: int = 0
    cull_removed: int = 0


_DEFAULT_TELEMETRY_STORE_LOCK = threading.Lock()
_DEFAULT_TELEMETRY_STORE: list[CacheTelemetryStore | None] = [None]
_MAX_KEY_SIZE_64 = 64
_MAX_KEY_SIZE_128 = 128
_MAX_KEY_SIZE_256 = 256


def _incr(namespace: str, field: str, amount: int = 1) -> None:
    get_default_cache_telemetry_store().incr(namespace, field, amount)


def get_default_cache_telemetry_store() -> CacheTelemetryStore:
    """Return process-default cache telemetry store."""
    with _DEFAULT_TELEMETRY_STORE_LOCK:
        store = _DEFAULT_TELEMETRY_STORE[0]
        if store is None:
            store = CacheTelemetryStore()
            _DEFAULT_TELEMETRY_STORE[0] = store
        return store


def set_default_cache_telemetry_store(store: CacheTelemetryStore | None) -> None:
    """Set or clear process-default telemetry store."""
    with _DEFAULT_TELEMETRY_STORE_LOCK:
        _DEFAULT_TELEMETRY_STORE[0] = store


def record_cache_key(*, namespace: str, key: str) -> None:
    """Record key cardinality and key-size telemetry."""
    get_default_cache_telemetry_store().record_cache_key(
        namespace=namespace,
        key=key,
        max_key_size_64=_MAX_KEY_SIZE_64,
        max_key_size_128=_MAX_KEY_SIZE_128,
        max_key_size_256=_MAX_KEY_SIZE_256,
    )


def record_cache_get(*, namespace: str, hit: bool, key: str | None = None) -> None:
    """Record one cache get and hit/miss outcome."""
    if key is not None:
        record_cache_key(namespace=namespace, key=key)
    _incr(namespace, "gets")
    _incr(namespace, "hits" if hit else "misses")


def record_cache_set(*, namespace: str, ok: bool, key: str | None = None) -> None:
    """Record one cache set outcome."""
    if key is not None:
        record_cache_key(namespace=namespace, key=key)
    _incr(namespace, "sets")
    if not ok:
        _incr(namespace, "set_failures")


def record_cache_delete(*, namespace: str, ok: bool, key: str | None = None) -> None:
    """Record one cache delete outcome."""
    if key is not None:
        record_cache_key(namespace=namespace, key=key)
    _incr(namespace, "deletes")
    if not ok:
        _incr(namespace, "delete_failures")


def record_cache_evict(*, namespace: str, ok: bool, tag: str | None = None) -> None:
    """Record one cache evict-tag outcome."""
    if tag is not None:
        record_cache_key(namespace=namespace, key=tag)
    _incr(namespace, "evictions")
    if not ok:
        _incr(namespace, "eviction_failures")


def record_cache_decode_failure(*, namespace: str) -> None:
    """Record one cache payload decode failure."""
    _incr(namespace, "decode_failures")


def record_cache_timeout(*, namespace: str) -> None:
    """Record one backend timeout event."""
    _incr(namespace, "timeouts")


def record_cache_abort(*, namespace: str) -> None:
    """Record one backend abort/fail-open event."""
    _incr(namespace, "aborts")


def record_cache_volume(*, namespace: str, volume_bytes: int) -> None:
    """Record backend volume snapshot for namespace."""
    get_default_cache_telemetry_store().set_volume(namespace=namespace, volume_bytes=volume_bytes)


def record_cache_cull(*, namespace: str, removed: int) -> None:
    """Record backend cull outcome."""
    _incr(namespace, "cull_calls")
    _incr(namespace, "cull_removed", amount=max(0, int(removed)))


def snapshot_cache_telemetry() -> dict[str, CacheNamespaceTelemetry]:
    """Return a snapshot of namespace counters."""
    snapshot = get_default_cache_telemetry_store().snapshot()
    output: dict[str, CacheNamespaceTelemetry] = {}
    for namespace, bucket in snapshot.items():
        output[namespace] = CacheNamespaceTelemetry(
            gets=int(bucket.get("gets", 0)),
            hits=int(bucket.get("hits", 0)),
            misses=int(bucket.get("misses", 0)),
            sets=int(bucket.get("sets", 0)),
            set_failures=int(bucket.get("set_failures", 0)),
            deletes=int(bucket.get("deletes", 0)),
            delete_failures=int(bucket.get("delete_failures", 0)),
            evictions=int(bucket.get("evictions", 0)),
            eviction_failures=int(bucket.get("eviction_failures", 0)),
            decode_failures=int(bucket.get("decode_failures", 0)),
            key_cardinality=int(bucket.get("key_cardinality", 0)),
            key_size_le_64=int(bucket.get("key_size_le_64", 0)),
            key_size_le_128=int(bucket.get("key_size_le_128", 0)),
            key_size_le_256=int(bucket.get("key_size_le_256", 0)),
            key_size_gt_256=int(bucket.get("key_size_gt_256", 0)),
            timeouts=int(bucket.get("timeouts", 0)),
            aborts=int(bucket.get("aborts", 0)),
            last_volume_bytes=int(bucket.get("last_volume_bytes", 0)),
            cull_calls=int(bucket.get("cull_calls", 0)),
            cull_removed=int(bucket.get("cull_removed", 0)),
        )
    return output


def reset_cache_telemetry() -> None:
    """Reset all namespace telemetry counters."""
    get_default_cache_telemetry_store().reset()


__all__ = [
    "CacheNamespaceTelemetry",
    "get_default_cache_telemetry_store",
    "record_cache_abort",
    "record_cache_cull",
    "record_cache_decode_failure",
    "record_cache_delete",
    "record_cache_evict",
    "record_cache_get",
    "record_cache_key",
    "record_cache_set",
    "record_cache_timeout",
    "record_cache_volume",
    "reset_cache_telemetry",
    "set_default_cache_telemetry_store",
    "snapshot_cache_telemetry",
]
