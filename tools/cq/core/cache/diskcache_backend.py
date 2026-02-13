"""Disk-backed CQ cache backend built on diskcache FanoutCache."""

from __future__ import annotations

import atexit
import shutil
import sqlite3
import threading
from pathlib import Path

from diskcache import FanoutCache

from tools.cq.core.cache.interface import CqCacheBackend, NoopCacheBackend
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy


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

    def get(self, key: str) -> object | None:
        """Fetch key from cache.

        Returns:
            Cached value when present, otherwise `None`.
        """
        try:
            return self._cache.get(key, default=None)
        except (OSError, RuntimeError, ValueError, TypeError):
            return None

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> None:
        """Write value to cache."""
        ttl = expire if expire is not None else self._default_ttl_seconds
        try:
            self._cache.set(key, value, expire=ttl, tag=tag, retry=True)
        except (OSError, RuntimeError, ValueError, TypeError):
            return

    def delete(self, key: str) -> None:
        """Delete key from cache."""
        try:
            self._cache.delete(key, retry=True)
        except (OSError, RuntimeError, ValueError, TypeError):
            return

    def evict_tag(self, tag: str) -> None:
        """Evict tagged items."""
        try:
            self._cache.evict(tag, retry=True)
        except (OSError, RuntimeError, ValueError, TypeError):
            return

    def close(self) -> None:
        """Close cache resources."""
        self._cache.close()


_BACKEND_LOCK = threading.Lock()


class _BackendState:
    """Mutable holder for process-global cache backend singleton."""

    def __init__(self) -> None:
        """Initialize empty backend state."""
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
        )

    try:
        cache = _open_cache()
    except (sqlite3.DatabaseError, OSError, RuntimeError, ValueError, TypeError):
        try:
            shutil.rmtree(directory, ignore_errors=True)
            cache = _open_cache()
        except (sqlite3.DatabaseError, OSError, RuntimeError, ValueError, TypeError):
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
