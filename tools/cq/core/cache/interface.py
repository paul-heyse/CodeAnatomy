"""Cache interface contracts for CQ runtime adapters."""

from __future__ import annotations

from typing import Protocol


class CqCacheBackend(Protocol):
    """Cache adapter contract used by CQ runtime services."""

    def get(self, key: str) -> object | None:
        """Fetch cached value for key.

        Returns:
            Cached value or `None` when absent.
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
    ) -> None:
        """Store value for key."""
        _ = (self, key, value, expire, tag)

    def delete(self, key: str) -> None:
        """Delete cached key."""
        _ = (self, key)

    def evict_tag(self, tag: str) -> None:
        """Evict entries associated with tag when supported."""
        _ = (self, tag)

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
    ) -> None:
        """Ignore writes."""
        _ = (self, key, value, expire, tag)

    def delete(self, key: str) -> None:
        """Ignore deletes."""
        _ = (self, key)

    def evict_tag(self, tag: str) -> None:
        """Ignore tag evictions."""
        _ = (self, tag)

    def close(self) -> None:
        """No resources to close."""


__all__ = ["CqCacheBackend", "NoopCacheBackend"]
