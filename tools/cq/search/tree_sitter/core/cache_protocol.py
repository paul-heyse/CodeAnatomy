"""Minimal cache protocol for tree-sitter adaptive runtime helpers."""

from __future__ import annotations

from typing import Protocol


class CacheBackendProtocol(Protocol):
    """Cache surface required by adaptive runtime helpers."""

    def get(self, key: str) -> object | None:
        """Return cached value for ``key`` when available."""
        ...

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Persist ``value`` for ``key`` and return backend acknowledgement."""
        ...


__all__ = ["CacheBackendProtocol"]
