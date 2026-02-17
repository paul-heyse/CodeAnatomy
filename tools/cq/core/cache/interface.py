"""Cache interface contracts for CQ runtime adapters."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol, runtime_checkable


class CqCacheReadWriteBackend(Protocol):
    """Core key/value cache capability."""

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

    def set_many(
        self,
        items: dict[str, object],
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> int:
        """Store multiple values and return successful write count.

        Returns:
            int: Number of values written.
        """
        _ = (self, items, expire, tag)
        return 0

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


class CqCacheMaintenanceBackend(Protocol):
    """Maintenance and lifecycle cache capability."""

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

    def touch(self, key: str, *, expire: int | None = None) -> bool:
        """Refresh key TTL when backend supports touch semantics.

        Returns:
            bool: ``True`` when TTL refresh succeeds, otherwise ``False``.
        """
        _ = (self, key, expire)
        return False

    def expire(self) -> int | None:
        """Trigger backend expiry cleanup when supported.

        Returns:
            int | None: Number of expired entries removed.
        """
        _ = self
        return None

    def check(self, *, fix: bool = False) -> int | None:
        """Run backend integrity check when supported.

        Returns:
            int | None: Integrity errors detected (or fixed) count.
        """
        _ = (self, fix)
        return None

    def close(self) -> None:
        """Close backend resources.

        Returns:
            None
        """


class CqCacheBackend(
    CqCacheReadWriteBackend,
    CqCacheMaintenanceBackend,
    Protocol,
):
    """Composed cache adapter contract used by CQ runtime services."""


@runtime_checkable
class CqCacheStreamingBackend(CqCacheBackend, Protocol):
    """Optional streaming/blob cache capability."""

    def read_streaming(self, key: str) -> bytes | None:
        """Read streaming payload for ``key`` when supported.

        Returns:
            bytes | None: Streamed payload, if available.
        """
        _ = (self, key)
        return None

    def set_streaming(
        self,
        key: str,
        payload: bytes,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Write streaming payload for ``key`` when supported.

        Returns:
            bool: ``True`` when the payload is stored.
        """
        _ = (self, key, payload, expire, tag)
        return False


@runtime_checkable
class CqCacheCoordinationBackend(CqCacheBackend, Protocol):
    """Optional coordination capability (lock/rlock/semaphore/barrier)."""

    def lock(self, key: str, *, expire: int | None = None) -> AbstractContextManager[None]:
        """Return lock context manager for ``key`` when supported."""
        _ = (self, key, expire)
        return nullcontext()

    def rlock(self, key: str, *, expire: int | None = None) -> AbstractContextManager[None]:
        """Return reentrant lock context manager for ``key`` when supported."""
        _ = (self, key, expire)
        return nullcontext()

    def semaphore(
        self,
        key: str,
        *,
        value: int,
        expire: int | None = None,
    ) -> AbstractContextManager[None]:
        """Return semaphore context manager for ``key`` when supported."""
        _ = (self, key, value, expire)
        return nullcontext()

    def barrier(self, key: str, publish_fn: Callable[[], None]) -> None:
        """Run ``publish_fn`` once under a named barrier when supported."""
        _ = (self, key)
        publish_fn()


class _NoopReadWriteMixin:
    """No-op key/value behavior."""

    def get(self, key: str) -> object | None:
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
        _ = (self, key, value, expire, tag)
        return False

    def set_many(
        self,
        items: dict[str, object],
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> int:
        _ = (self, items, expire, tag)
        return 0

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        _ = (self, key, delta, default)
        return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        _ = (self, key, delta, default)
        return None

    def delete(self, key: str) -> bool:
        _ = (self, key)
        return False

    def evict_tag(self, tag: str) -> bool:
        _ = (self, tag)
        return False


class _NoopMaintenanceMixin:
    """No-op maintenance and lifecycle behavior."""

    def transact(self) -> AbstractContextManager[None]:
        _ = self
        return nullcontext()

    def stats(self) -> dict[str, object]:
        _ = self
        return {}

    def volume(self) -> int | None:
        _ = self
        return None

    def cull(self) -> int | None:
        _ = self
        return None

    def touch(self, key: str, *, expire: int | None = None) -> bool:
        _ = (self, key, expire)
        return False

    def expire(self) -> int | None:
        _ = self
        return None

    def check(self, *, fix: bool = False) -> int | None:
        _ = (self, fix)
        return None

    def close(self) -> None:
        """No resources to close."""


class _NoopStreamingMixin:
    """No-op streaming behavior."""

    def read_streaming(self, key: str) -> bytes | None:
        _ = (self, key)
        return None

    def set_streaming(
        self,
        key: str,
        payload: bytes,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = (self, key, payload, expire, tag)
        return False


class _NoopCoordinationMixin:
    """No-op coordination behavior."""

    def lock(self, key: str, *, expire: int | None = None) -> AbstractContextManager[None]:
        _ = (self, key, expire)
        return nullcontext()

    def rlock(self, key: str, *, expire: int | None = None) -> AbstractContextManager[None]:
        _ = (self, key, expire)
        return nullcontext()

    def semaphore(
        self,
        key: str,
        *,
        value: int,
        expire: int | None = None,
    ) -> AbstractContextManager[None]:
        _ = (self, key, value, expire)
        return nullcontext()

    def barrier(self, key: str, publish_fn: Callable[[], None]) -> None:
        _ = (self, key)
        publish_fn()


class NoopCacheBackend(
    _NoopReadWriteMixin,
    _NoopMaintenanceMixin,
    _NoopStreamingMixin,
    _NoopCoordinationMixin,
):
    """No-op cache backend used when caching is disabled."""


__all__ = [
    "CqCacheBackend",
    "CqCacheCoordinationBackend",
    "CqCacheStreamingBackend",
    "NoopCacheBackend",
]
