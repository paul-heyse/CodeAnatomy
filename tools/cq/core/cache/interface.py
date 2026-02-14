"""Cache interface contracts for CQ runtime adapters."""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from typing import Protocol


class CqCacheBackend(Protocol):
    """Cache adapter contract used by CQ runtime services."""

    def get(self, key: str) -> object | None:
        """Fetch cached value for key.

        Returns:
            object | None: Cached value for key, or `None` if missing.
        """
        _ = (self, key)
        return None

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Store value for key and return write acknowledgement.

        Returns:
            bool: `True` if the operation would persist the value, else `False`.
        """
        _ = (self, key, value, expire, tag)
        return False

    def add(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Store value only if key is absent and return acknowledgement.

        Returns:
            bool: `True` if the add operation would happen, else `False`.
        """
        _ = (self, key, value, expire, tag)
        return False

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Increment numeric key by delta.

        Returns:
            int | None: Incremented value, or `None` when unavailable.
        """
        _ = (self, key, delta, default)
        return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Decrement numeric key by delta.

        Returns:
            int | None: Decremented value, or `None` when unavailable.
        """
        _ = (self, key, delta, default)
        return None

    def delete(self, key: str) -> bool:
        """Delete cached key and return acknowledgement.

        Returns:
            bool: `True` if delete would proceed, else `False`.
        """
        _ = (self, key)
        return False

    def evict_tag(self, tag: str) -> bool:
        """Evict entries associated with tag when supported.

        Returns:
            bool: `True` when eviction succeeds, else `False`.
        """
        _ = (self, tag)
        return False

    def transact(self) -> AbstractContextManager[None]:
        """Return transaction context manager when supported.

        Returns:
            ContextManager[None]: Transaction context manager.
        """
        _ = self
        return nullcontext()

    def stats(self) -> dict[str, object]:
        """Return backend-level cache statistics.

        Returns:
            dict[str, object]: Backend metrics keyed by metric name.
        """
        _ = self
        return {}

    def volume(self) -> int | None:
        """Return backend storage volume estimate when supported.

        Returns:
            int | None: Cache volume estimate in bytes, or `None`.
        """
        _ = self
        return None

    def cull(self) -> int | None:
        """Trigger backend cull/maintenance when supported.

        Returns:
            int | None: Number of entries removed, or `None`.
        """
        _ = self
        return None

    def close(self) -> None:
        """Close backend resources.

        Returns:
            None
        """


class NoopCacheBackend:
    """No-op cache backend used when caching is disabled."""

    def get(self, key: str) -> object | None:
        """Return no value.

        Returns:
            object | None: Always `None` in no-op backend.
        """
        _ = (self, key)
        return None

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Ignore writes.

        Returns:
            bool: Always `False` in no-op backend.
        """
        _ = (self, key, value, expire, tag)
        return False

    def add(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Ignore add operations.

        Returns:
            bool: Always `False` in no-op backend.
        """
        _ = (self, key, value, expire, tag)
        return False

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Ignore increments.

        Returns:
            int | None: Always `None` in no-op backend.
        """
        _ = (self, key, delta, default)
        return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Ignore decrements.

        Returns:
            int | None: Always `None` in no-op backend.
        """
        _ = (self, key, delta, default)
        return None

    def delete(self, key: str) -> bool:
        """Ignore deletes.

        Returns:
            bool: Always `False` in no-op backend.
        """
        _ = (self, key)
        return False

    def evict_tag(self, tag: str) -> bool:
        """Ignore tag evictions.

        Returns:
            bool: Always `False` in no-op backend.
        """
        _ = (self, tag)
        return False

    def transact(self) -> AbstractContextManager[None]:
        """Return no-op transaction context manager.

        Returns:
            ContextManager[None]: Null context manager.
        """
        _ = self
        return nullcontext()

    def stats(self) -> dict[str, object]:
        """Return empty stats payload.

        Returns:
            dict[str, object]: Empty stats payload.
        """
        _ = self
        return {}

    def volume(self) -> int | None:
        """Return no volume payload.

        Returns:
            int | None: Always `None` in no-op backend.
        """
        _ = self
        return None

    def cull(self) -> int | None:
        """No-op cull.

        Returns:
            int | None: Always `None` in no-op backend.
        """
        _ = self
        return None

    def close(self) -> None:
        """No resources to close.

        Returns:
            None
        """


__all__ = ["CqCacheBackend", "NoopCacheBackend"]
