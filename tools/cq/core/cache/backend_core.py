"""Low-level cache backend operation helpers."""

from __future__ import annotations

from tools.cq.core.cache.interface import CqCacheBackend

__all__ = ["backend_delete", "backend_get", "backend_set"]


def backend_get(backend: CqCacheBackend, key: str) -> object | None:
    """Get one key from cache backend.

    Returns:
        object | None: Cached value if present, else ``None``.
    """
    return backend.get(key)


def backend_set(
    backend: CqCacheBackend,
    key: str,
    value: object,
    *,
    expire: int | None = None,
    tag: str | None = None,
) -> bool:
    """Set one key on cache backend.

    Returns:
        bool: ``True`` when the value was persisted by the backend.
    """
    return backend.set(key, value, expire=expire, tag=tag)


def backend_delete(backend: CqCacheBackend, key: str) -> bool:
    """Delete one key from cache backend.

    Returns:
        bool: ``True`` when a key was removed.
    """
    return backend.delete(key)
