"""Process-local cache telemetry counters for CQ namespaces."""

from __future__ import annotations

import threading

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


_TELEMETRY_LOCK = threading.Lock()
_TELEMETRY: dict[str, dict[str, int]] = {}
_SEEN_KEYS: dict[str, set[str]] = {}
_MAX_KEY_SIZE_64 = 64
_MAX_KEY_SIZE_128 = 128
_MAX_KEY_SIZE_256 = 256


def _bucket(namespace: str) -> dict[str, int]:
    bucket = _TELEMETRY.get(namespace)
    if bucket is None:
        bucket = {}
        _TELEMETRY[namespace] = bucket
    return bucket


def _incr(namespace: str, field: str, amount: int = 1) -> None:
    with _TELEMETRY_LOCK:
        bucket = _bucket(namespace)
        bucket[field] = int(bucket.get(field, 0)) + int(amount)


def record_cache_key(*, namespace: str, key: str) -> None:
    """Record key cardinality and key-size telemetry."""
    key_len = len(key)
    with _TELEMETRY_LOCK:
        seen = _SEEN_KEYS.setdefault(namespace, set())
        seen.add(key)
        bucket = _bucket(namespace)
        bucket["key_cardinality"] = len(seen)
    if key_len <= _MAX_KEY_SIZE_64:
        _incr(namespace, "key_size_le_64")
    elif key_len <= _MAX_KEY_SIZE_128:
        _incr(namespace, "key_size_le_128")
    elif key_len <= _MAX_KEY_SIZE_256:
        _incr(namespace, "key_size_le_256")
    else:
        _incr(namespace, "key_size_gt_256")


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
    with _TELEMETRY_LOCK:
        bucket = _bucket(namespace)
        bucket["last_volume_bytes"] = max(0, int(volume_bytes))


def record_cache_cull(*, namespace: str, removed: int) -> None:
    """Record backend cull outcome."""
    _incr(namespace, "cull_calls")
    _incr(namespace, "cull_removed", amount=max(0, int(removed)))


def snapshot_cache_telemetry() -> dict[str, CacheNamespaceTelemetry]:
    """Return a snapshot of namespace counters."""
    with _TELEMETRY_LOCK:
        snapshot = {namespace: dict(bucket) for namespace, bucket in _TELEMETRY.items()}
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
    with _TELEMETRY_LOCK:
        _TELEMETRY.clear()
        _SEEN_KEYS.clear()


__all__ = [
    "CacheNamespaceTelemetry",
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
    "snapshot_cache_telemetry",
]
