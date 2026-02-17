"""State store for cache telemetry counters."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class CacheTelemetryStore:
    """Thread-safe process-local cache telemetry store."""

    telemetry: dict[str, dict[str, int]] = field(default_factory=dict)
    seen_keys: dict[str, set[str]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _bucket(self, namespace: str) -> dict[str, int]:
        bucket = self.telemetry.get(namespace)
        if bucket is None:
            bucket = {}
            self.telemetry[namespace] = bucket
        return bucket

    def _incr(self, namespace: str, field: str, amount: int = 1) -> None:
        with self._lock:
            bucket = self._bucket(namespace)
            bucket[field] = int(bucket.get(field, 0)) + int(amount)

    def incr(self, namespace: str, field: str, amount: int = 1) -> None:
        """Increment one telemetry counter for a namespace."""
        self._incr(namespace, field, amount)

    def record_cache_key(
        self,
        *,
        namespace: str,
        key: str,
        max_key_size_64: int,
        max_key_size_128: int,
        max_key_size_256: int,
    ) -> None:
        """Record key-cardinality and key-size bucket telemetry for a namespace."""
        key_len = len(key)
        with self._lock:
            seen = self.seen_keys.setdefault(namespace, set())
            seen.add(key)
            bucket = self._bucket(namespace)
            bucket["key_cardinality"] = len(seen)
        if key_len <= max_key_size_64:
            self._incr(namespace, "key_size_le_64")
        elif key_len <= max_key_size_128:
            self._incr(namespace, "key_size_le_128")
        elif key_len <= max_key_size_256:
            self._incr(namespace, "key_size_le_256")
        else:
            self._incr(namespace, "key_size_gt_256")

    def snapshot(self) -> dict[str, dict[str, int]]:
        """Return a thread-safe snapshot copy of telemetry counters."""
        with self._lock:
            return {namespace: dict(bucket) for namespace, bucket in self.telemetry.items()}

    def set_volume(self, *, namespace: str, volume_bytes: int) -> None:
        """Record latest observed cache volume in bytes for a namespace."""
        with self._lock:
            bucket = self._bucket(namespace)
            bucket["last_volume_bytes"] = max(0, int(volume_bytes))

    def reset(self) -> None:
        """Clear telemetry counters and key-tracking state."""
        with self._lock:
            self.telemetry.clear()
            self.seen_keys.clear()


__all__ = ["CacheTelemetryStore"]
