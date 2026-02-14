"""Cache interface contracts for CQ runtime adapters."""

from __future__ import annotations

from contextlib import nullcontext
from typing import ContextManager, Protocol


class CqCacheBackend(Protocol):
    """Cache adapter contract used by CQ runtime services."""

    def get(self, key: str) -> object | None:
        """Fetch cached value for key."""
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
        """Store value for key and return write acknowledgement."""
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
        """Store value only if key is absent and return acknowledgement."""
        _ = (self, key, value, expire, tag)
        return False

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Increment numeric key by delta."""
        _ = (self, key, delta, default)
        return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Decrement numeric key by delta."""
        _ = (self, key, delta, default)
        return None

    def delete(self, key: str) -> bool:
        """Delete cached key and return acknowledgement."""
        _ = (self, key)
        return False

    def evict_tag(self, tag: str) -> bool:
        """Evict entries associated with tag when supported."""
        _ = (self, tag)
        return False

    def transact(self) -> ContextManager[None]:
        """Return transaction context manager when supported."""
        return nullcontext()

    def stats(self) -> dict[str, object]:
        """Return backend-level cache statistics."""
        return {}

    def volume(self) -> int | None:
        """Return backend storage volume estimate when supported."""
        return None

    def cull(self) -> int | None:
        """Trigger backend cull/maintenance when supported."""
        return None

    def close(self) -> None:
        """Close backend resources."""


class NoopCacheBackend:
    """No-op cache backend used when caching is disabled."""

    def get(self, key: str) -> object | None:
        """Return no value."""
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
        """Ignore writes."""
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
        """Ignore add operations."""
        _ = (self, key, value, expire, tag)
        return False

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Ignore increments."""
        _ = (self, key, delta, default)
        return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Ignore decrements."""
        _ = (self, key, delta, default)
        return None

    def delete(self, key: str) -> bool:
        """Ignore deletes."""
        _ = (self, key)
        return False

    def evict_tag(self, tag: str) -> bool:
        """Ignore tag evictions."""
        _ = (self, tag)
        return False

    def transact(self) -> ContextManager[None]:
        """Return no-op transaction context manager."""
        return nullcontext()

    def stats(self) -> dict[str, object]:
        """Return empty stats payload."""
        return {}

    def volume(self) -> int | None:
        """Return no volume payload."""
        return None

    def cull(self) -> int | None:
        """No-op cull."""
        return None

    def close(self) -> None:
        """No resources to close."""


__all__ = ["CqCacheBackend", "NoopCacheBackend"]
