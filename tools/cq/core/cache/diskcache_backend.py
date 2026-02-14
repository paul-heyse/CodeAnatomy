"""Disk-backed CQ cache backend built on diskcache FanoutCache."""

from __future__ import annotations

import atexit
import shutil
import sqlite3
import threading
from pathlib import Path
from typing import Final

from diskcache import FanoutCache, Timeout

from tools.cq.core.cache.interface import CqCacheBackend, NoopCacheBackend
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.telemetry import (
    record_cache_abort,
    record_cache_cull,
    record_cache_timeout,
    record_cache_volume,
)

_NON_FATAL_ERRORS: Final = (
    OSError,
    RuntimeError,
    ValueError,
    TypeError,
    sqlite3.DatabaseError,
)
_OPEN_ERRORS: Final = (Timeout, *_NON_FATAL_ERRORS)


class _FailOpenTransaction:
    """Fail-open transaction wrapper that degrades to no-op on backend errors."""

    def __init__(self, backend: DiskcacheBackend) -> None:
        self._backend = backend
        self._ctx: object | None = None

    def __enter__(self) -> None:
        try:
            ctx = self._backend._cache.transact(retry=True)
            self._ctx = ctx
            enter = getattr(ctx, "__enter__", None)
            if callable(enter):
                enter()
        except Timeout:
            self._backend._record_timeout(namespace="transaction")
            self._ctx = None
        except _NON_FATAL_ERRORS:
            self._backend._record_abort(namespace="transaction")
            self._ctx = None

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        if self._ctx is None:
            return False
        exit_fn = getattr(self._ctx, "__exit__", None)
        if not callable(exit_fn):
            return False
        try:
            return bool(exit_fn(exc_type, exc, tb))
        except Timeout:
            self._backend._record_timeout(namespace="transaction")
            return False
        except _NON_FATAL_ERRORS:
            self._backend._record_abort(namespace="transaction")
            return False


class DiskcacheBackend:
    """Fail-open diskcache backend for CQ runtime."""

    def __init__(self, cache: FanoutCache, *, default_ttl_seconds: int) -> None:
        """Initialize disk cache adapter.

        Args:
            cache: Backing diskcache instance.
            default_ttl_seconds: Default cache TTL in seconds.
        """
        self._cache = cache
        self._default_ttl_seconds = default_ttl_seconds

    @staticmethod
    def _namespace_from_key(key: str) -> str:
        if key.startswith("cq:"):
            parts = key.split(":", maxsplit=3)
            if len(parts) >= 2 and parts[1]:
                return parts[1]
        return "cache_backend"

    @staticmethod
    def _namespace_from_tag(tag: str) -> str:
        for atom in tag.split("|"):
            if atom.startswith("ns:") and len(atom) > 3:
                return atom[3:]
        return "cache_backend"

    def _record_timeout(self, *, namespace: str) -> None:
        record_cache_timeout(namespace=namespace)

    def _record_abort(self, *, namespace: str) -> None:
        record_cache_abort(namespace=namespace)

    def get(self, key: str) -> object | None:
        """Fetch key from cache.

        Returns:
            object | None: Cached value for the key, or `None` on miss/error.
        """
        namespace = self._namespace_from_key(key)
        try:
            return self._cache.get(key, default=None, retry=True)
        except Timeout:
            self._record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return None

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Write value to cache and return acknowledgement.

        Returns:
            bool: `True` when the value was written, `False` otherwise.
        """
        ttl = expire if expire is not None else self._default_ttl_seconds
        namespace = self._namespace_from_key(key)
        try:
            return bool(self._cache.set(key, value, expire=ttl, tag=tag, retry=True))
        except Timeout:
            self._record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return False

    def add(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Write value only when absent and return acknowledgement.

        Returns:
            bool: `True` when the value was added, `False` otherwise.
        """
        ttl = expire if expire is not None else self._default_ttl_seconds
        namespace = self._namespace_from_key(key)
        try:
            return bool(self._cache.add(key, value, expire=ttl, tag=tag, retry=True))
        except Timeout:
            self._record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return False

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Increment numeric value and return updated value.

        Returns:
            int | None: Updated integer value when present, otherwise `None`.
        """
        namespace = self._namespace_from_key(key)
        try:
            value = self._cache.incr(key, delta=delta, default=default, retry=True)
            if value is None:
                return None
            return int(value)
        except Timeout:
            self._record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Decrement numeric value and return updated value.

        Returns:
            int | None: Updated integer value when present, otherwise `None`.
        """
        namespace = self._namespace_from_key(key)
        try:
            value = self._cache.decr(key, delta=delta, default=default, retry=True)
            if value is None:
                return None
            return int(value)
        except Timeout:
            self._record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return None

    def delete(self, key: str) -> bool:
        """Delete key from cache and return acknowledgement.

        Returns:
            bool: `True` when deletion was attempted successfully, `False` otherwise.
        """
        namespace = self._namespace_from_key(key)
        try:
            return bool(self._cache.delete(key, retry=True))
        except Timeout:
            self._record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return False

    def evict_tag(self, tag: str) -> bool:
        """Evict tagged items and return acknowledgement.

        Returns:
            bool: `True` when tag eviction succeeded, `False` otherwise.
        """
        namespace = self._namespace_from_tag(tag)
        try:
            self._cache.evict(tag, retry=True)
            return True
        except Timeout:
            self._record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace=namespace)
            return False

    def transact(self) -> _FailOpenTransaction:
        """Return fail-open transaction context manager.

        Returns:
            _FailOpenTransaction: Context manager for best-effort transactional scope.
        """
        return _FailOpenTransaction(self)

    def stats(self) -> dict[str, object]:
        """Return cache stats payload.

        Returns:
            dict[str, object]: Cache hit/miss counts and related metrics.
        """
        try:
            hits, misses = self._cache.stats(enable=False, reset=False)
            return {
                "hits": int(hits),
                "misses": int(misses),
            }
        except Timeout:
            self._record_timeout(namespace="cache_backend")
            return {}
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace="cache_backend")
            return {}

    def volume(self) -> int | None:
        """Return backend volume in bytes, when available.

        Returns:
            int | None: Estimated cache volume in bytes, or `None` on error.
        """
        try:
            volume_bytes = int(self._cache.volume())
            record_cache_volume(namespace="cache_backend", volume_bytes=volume_bytes)
            return volume_bytes
        except Timeout:
            self._record_timeout(namespace="cache_backend")
            return None
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace="cache_backend")
            return None

    def cull(self) -> int | None:
        """Trigger backend cull and return number of removed entries.

        Returns:
            int | None: Number of entries removed, or `None` on error.
        """
        try:
            removed = int(self._cache.cull(retry=True))
            record_cache_cull(namespace="cache_backend", removed=removed)
            return removed
        except Timeout:
            self._record_timeout(namespace="cache_backend")
            return None
        except _NON_FATAL_ERRORS:
            self._record_abort(namespace="cache_backend")
            return None

    def close(self) -> None:
        """Close cache resources."""
        try:
            self._cache.close()
        except _NON_FATAL_ERRORS:
            return


_BACKEND_LOCK = threading.Lock()


class _BackendState:
    """Mutable holder for process-global cache backend singleton."""

    def __init__(self) -> None:
        self.backends: dict[str, CqCacheBackend] = {}


_BACKEND_STATE = _BackendState()


def _build_diskcache_backend(policy: CqCachePolicyV1) -> CqCacheBackend:
    directory = Path(policy.directory).expanduser()

    def _open_cache() -> FanoutCache:
        return FanoutCache(
            directory=str(directory),
            shards=max(1, int(policy.shards)),
            timeout=float(policy.timeout_seconds),
            tag_index=True,
            statistics=bool(policy.statistics_enabled),
            size_limit=max(1, int(policy.size_limit_bytes)),
            cull_limit=max(0, int(policy.cull_limit)),
            eviction_policy=policy.eviction_policy,
        )

    try:
        cache = _open_cache()
    except _OPEN_ERRORS:
        try:
            shutil.rmtree(directory, ignore_errors=True)
            cache = _open_cache()
        except _OPEN_ERRORS:
            return NoopCacheBackend()
    return DiskcacheBackend(cache, default_ttl_seconds=policy.ttl_seconds)


def get_cq_cache_backend(*, root: Path) -> CqCacheBackend:
    """Return workspace-keyed CQ cache backend."""
    workspace = str(root.resolve())
    with _BACKEND_LOCK:
        existing = _BACKEND_STATE.backends.get(workspace)
        if existing is not None:
            return existing
        policy = default_cache_policy(root=root)
        if not policy.enabled:
            backend: CqCacheBackend = NoopCacheBackend()
        else:
            backend = _build_diskcache_backend(policy)
        _BACKEND_STATE.backends[workspace] = backend
        return backend


def close_cq_cache_backend(*, root: Path | None = None) -> None:
    """Close and clear workspace-backed cache backend(s)."""
    backends: list[CqCacheBackend]
    with _BACKEND_LOCK:
        if root is None:
            backends = list(_BACKEND_STATE.backends.values())
            _BACKEND_STATE.backends.clear()
        else:
            workspace = str(root.resolve())
            backend = _BACKEND_STATE.backends.pop(workspace, None)
            backends = [backend] if backend is not None else []
    for backend in backends:
        backend.close()


atexit.register(close_cq_cache_backend)


__all__ = [
    "DiskcacheBackend",
    "close_cq_cache_backend",
    "get_cq_cache_backend",
]
