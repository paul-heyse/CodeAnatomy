"""Application service wrapper for smart search execution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.language import QueryLanguageScope
from tools.cq.search.classifier import QueryMode
from tools.cq.search.profiles import SearchLimits
from tools.cq.search.smart_search import smart_search


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


class SearchService:
    """Application-layer service for CQ search."""

    @staticmethod
    def execute(request: SearchServiceRequest) -> CqResult:
        """Execute CQ smart search.

        Returns:
            Search result payload.
        """
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


__all__ = ["SearchService", "SearchServiceRequest"]
