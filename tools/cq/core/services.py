"""CQ application services."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.enrichment_mode import (
    IncrementalEnrichmentModeV1,
    parse_incremental_enrichment_mode,
)
from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct
from tools.cq.macros.contracts import CallsRequest

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.core.types import QueryLanguageScope
    from tools.cq.search._shared.types import SearchLimits

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50


class EntityFrontDoorRequest(CqStruct, frozen=True):
    """Typed request contract for entity front-door attachment."""

    result: CqResult
    relationship_detail_max_matches: int = _ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES


class CallsServiceRequest(CqStruct, frozen=True):
    """Typed request contract for calls macro service execution."""

    request: CallsRequest


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
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None
    run_id: str | None = None
    incremental_enrichment_enabled: bool = True
    incremental_enrichment_mode: IncrementalEnrichmentModeV1 = IncrementalEnrichmentModeV1.TS_SYM


class EntityService:
    """Application-layer service for CQ entity flow."""

    @staticmethod
    def attach_front_door(request: EntityFrontDoorRequest) -> CqResult:
        """Attach entity front-door insight to a CQ result.

        Returns:
            CqResult: Updated result including front-door insight payload.
        """
        from tools.cq.query.entity_front_door import attach_entity_front_door_insight

        return attach_entity_front_door_insight(
            request.result,
            relationship_detail_max_matches=request.relationship_detail_max_matches,
        )


class CallsService:
    """Application-layer service for CQ calls macro."""

    @staticmethod
    def execute(request: CallsServiceRequest) -> CqResult:
        """Execute CQ calls macro.

        Returns:
            Calls macro result payload.
        """
        from tools.cq.macros.calls import cmd_calls

        return cmd_calls(request.request)


class SearchService:
    """Application-layer service for CQ search."""

    @staticmethod
    def execute(request: SearchServiceRequest) -> CqResult:
        """Execute CQ smart search.

        Returns:
            Search result payload.
        """
        from tools.cq.search._shared.types import QueryMode
        from tools.cq.search.pipeline.smart_search import smart_search

        parsed_mode: QueryMode | None = None
        if isinstance(request.mode, QueryMode):
            parsed_mode = request.mode
        elif isinstance(request.mode, str):
            normalized_mode = request.mode.strip().lower()
            if normalized_mode in {"identifier", "regex", "literal"}:
                parsed_mode = QueryMode(normalized_mode)

        return smart_search(
            root=request.root,
            query=request.query,
            mode=parsed_mode,
            lang_scope=request.lang_scope,
            include_globs=request.include_globs,
            exclude_globs=request.exclude_globs,
            include_strings=request.include_strings,
            with_neighborhood=request.with_neighborhood,
            limits=request.limits,
            tc=request.tc,
            argv=request.argv,
            run_id=request.run_id,
            incremental_enrichment_enabled=request.incremental_enrichment_enabled,
            incremental_enrichment_mode=parse_incremental_enrichment_mode(
                request.incremental_enrichment_mode
            ),
        )


__all__ = [
    "CallsService",
    "CallsServiceRequest",
    "EntityFrontDoorRequest",
    "EntityService",
    "SearchService",
    "SearchServiceRequest",
]
