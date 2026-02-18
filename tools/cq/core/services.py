"""CQ application services."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.contracts import CallsMacroRequestV1
from tools.cq.core.enrichment_mode import (
    IncrementalEnrichmentModeV1,
)
from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.core.types import QueryLanguageScope

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50


class EntityFrontDoorRequest(CqStruct, frozen=True):
    """Typed request contract for entity front-door attachment."""

    result: CqResult
    relationship_detail_max_matches: int = _ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES


class CallsServiceRequest(CqStruct, frozen=True):
    """Typed request contract for calls macro service execution."""

    request: CallsMacroRequestV1


class SearchServiceRequest(CqStruct, frozen=True):
    """Typed request contract for smart-search service execution."""

    root: Path
    query: str
    mode: str | None = None
    lang_scope: QueryLanguageScope = "auto"
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    with_neighborhood: bool = False
    limits: object | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None
    run_id: str | None = None
    incremental_enrichment_enabled: bool = True
    incremental_enrichment_mode: IncrementalEnrichmentModeV1 = IncrementalEnrichmentModeV1.TS_SYM


class EntityService:
    """Application-layer service for CQ entity flow."""

    def __init__(
        self,
        *,
        attach_front_door_fn: Callable[[EntityFrontDoorRequest], CqResult],
    ) -> None:
        """Initialize entity service with an injected front-door adapter."""
        self._attach_front_door_fn = attach_front_door_fn

    def attach_front_door(self, request: EntityFrontDoorRequest) -> CqResult:
        """Attach entity front-door insight to a CQ result.

        Returns:
            CqResult: Updated result including front-door insight payload.
        """
        return self._attach_front_door_fn(request)


class CallsService:
    """Application-layer service for CQ calls macro."""

    def __init__(self, *, execute_fn: Callable[[CallsServiceRequest], CqResult]) -> None:
        """Initialize calls service with an injected execution function."""
        self._execute_fn = execute_fn

    def execute(self, request: CallsServiceRequest) -> CqResult:
        """Execute CQ calls macro.

        Returns:
            Calls macro result payload.
        """
        return self._execute_fn(request)


class SearchService:
    """Application-layer service for CQ search."""

    def __init__(self, *, execute_fn: Callable[[SearchServiceRequest], CqResult]) -> None:
        """Initialize search service with an injected execution function."""
        self._execute_fn = execute_fn

    def execute(self, request: SearchServiceRequest) -> CqResult:
        """Execute CQ smart search.

        Returns:
            Search result payload.
        """
        return self._execute_fn(request)


__all__ = [
    "CallsService",
    "CallsServiceRequest",
    "EntityFrontDoorRequest",
    "EntityService",
    "SearchService",
    "SearchServiceRequest",
]
