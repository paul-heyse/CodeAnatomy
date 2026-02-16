"""Hexagonal ports for CQ runtime services."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from tools.cq.core.schema import CqResult
from tools.cq.core.services import (
    CallsServiceRequest,
    EntityFrontDoorRequest,
    SearchServiceRequest,
)
from tools.cq.query.language import QueryLanguage


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


class RenderEnrichmentPort(Protocol):
    """Port for render-time enrichment of findings from anchor context."""

    def enrich_anchor(
        self,
        *,
        root: Path,
        file: str,
        line: int,
        col: int,
        language: QueryLanguage,
        candidates: list[str],
    ) -> dict[str, object]:
        """Build enrichment payload for one anchor."""
        ...


__all__ = [
    "CachePort",
    "CallsServicePort",
    "EntityServicePort",
    "RenderEnrichmentPort",
    "SearchServicePort",
]
