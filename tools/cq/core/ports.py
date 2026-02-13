"""Hexagonal ports for CQ runtime services."""

from __future__ import annotations

from typing import Protocol

from tools.cq.core.schema import CqResult
from tools.cq.core.services import (
    CallsServiceRequest,
    EntityFrontDoorRequest,
    SearchServiceRequest,
)


class SearchServicePort(Protocol):
    """Port for search command execution."""

    def execute(self, request: SearchServiceRequest) -> CqResult:
        """Execute a search request and return CQ result."""
        ...


class EntityServicePort(Protocol):
    """Port for entity query execution."""

    def attach_front_door(self, request: EntityFrontDoorRequest) -> None:
        """Attach entity front-door insight to result."""
        ...


class CallsServicePort(Protocol):
    """Port for calls macro execution."""

    def execute(self, request: CallsServiceRequest) -> CqResult:
        """Execute a calls request and return CQ result."""
        ...


class CachePort(Protocol):
    """Port for cache get/set operations."""

    def get(self, key: str) -> object | None:
        """Read key from cache."""
        ...

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> None:
        """Write key/value into cache."""
        ...


__all__ = [
    "CachePort",
    "CallsServicePort",
    "EntityServicePort",
    "SearchServicePort",
]
