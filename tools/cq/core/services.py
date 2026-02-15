"""CQ application services."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.language import QueryLanguageScope
    from tools.cq.search.pipeline.classifier import QueryMode
    from tools.cq.search.pipeline.profiles import SearchLimits

_ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES = 50


class EntityFrontDoorRequest(CqStruct, frozen=True):
    """Typed request contract for entity front-door attachment."""

    result: CqResult
    relationship_detail_max_matches: int = _ENTITY_RELATIONSHIP_DETAIL_MAX_MATCHES


class CallsServiceRequest(CqStruct, frozen=True):
    """Typed request contract for calls macro service execution."""

    root: Path
    function_name: str
    tc: Toolchain
    argv: list[str]


class SearchServiceRequest(CqStruct, frozen=True):
    """Typed request contract for smart-search service execution."""

    root: Path
    query: str
    mode: QueryMode | None = None
    lang_scope: QueryLanguageScope = "auto"
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    with_neighborhood: bool = False
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None
    run_id: str | None = None


class EntityService:
    """Application-layer service for CQ entity flow."""

    @staticmethod
    def attach_front_door(request: EntityFrontDoorRequest) -> None:
        """Attach entity front-door insight to a CQ result."""
        from tools.cq.query.entity_front_door import attach_entity_front_door_insight

        attach_entity_front_door_insight(
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

        return cmd_calls(
            tc=request.tc,
            root=request.root,
            argv=request.argv,
            function_name=request.function_name,
        )


class SearchService:
    """Application-layer service for CQ search."""

    @staticmethod
    def execute(request: SearchServiceRequest) -> CqResult:
        """Execute CQ smart search.

        Returns:
            Search result payload.
        """
        from tools.cq.search.pipeline.smart_search import smart_search

        return smart_search(
            root=request.root,
            query=request.query,
            mode=request.mode,
            lang_scope=request.lang_scope,
            include_globs=request.include_globs,
            exclude_globs=request.exclude_globs,
            include_strings=request.include_strings,
            with_neighborhood=request.with_neighborhood,
            limits=request.limits,
            tc=request.tc,
            argv=request.argv,
            run_id=request.run_id,
        )


__all__ = [
    "CallsService",
    "CallsServiceRequest",
    "EntityFrontDoorRequest",
    "EntityService",
    "SearchService",
    "SearchServiceRequest",
]
