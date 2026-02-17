"""Query/search routing helpers for CLI command boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.result_factory import build_error_result
from tools.cq.core.schema import CqResult, ms
from tools.cq.orchestration.request_factory import (
    RequestContextV1,
    RequestFactory,
    SearchRequestOptionsV1,
)

if TYPE_CHECKING:
    from tools.cq.core.bootstrap import CqRuntimeServices
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.ir import Query


@dataclass(frozen=True)
class QueryRoutingResult:
    """Outcome contract for query/search routing."""

    parsed_query: Query | None = None
    result: CqResult | None = None


def route_query_or_search(
    query_string: str,
    *,
    root: Path,
    argv: list[str],
    toolchain: Toolchain | None,
    services: CqRuntimeServices,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> QueryRoutingResult:
    """Route one raw query string to declarative query or search fallback.

    Returns:
        QueryRoutingResult: Parsed query or search-fallback result envelope.
    """
    from tools.cq.core.types import DEFAULT_QUERY_LANGUAGE_SCOPE
    from tools.cq.query.parser import QueryParseError, has_query_tokens, parse_query
    from tools.cq.search.pipeline.smart_search import SMART_SEARCH_LIMITS

    has_tokens = has_query_tokens(query_string)
    try:
        return QueryRoutingResult(parsed_query=parse_query(query_string))
    except QueryParseError as error:
        if has_tokens:
            return QueryRoutingResult(
                result=build_error_result(
                    macro="q",
                    root=root,
                    argv=argv,
                    tc=toolchain,
                    started_ms=ms(),
                    error=error,
                )
            )

        resolved_toolchain = toolchain
        if resolved_toolchain is None:
            from tools.cq.core.toolchain import Toolchain

            resolved_toolchain = Toolchain.detect()
        request_ctx = RequestContextV1(root=root, argv=argv, tc=resolved_toolchain)
        request = RequestFactory.search(
            request_ctx,
            query=query_string,
            options=SearchRequestOptionsV1(
                mode=None,
                lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
                include_globs=include,
                exclude_globs=exclude,
                include_strings=False,
                limits=SMART_SEARCH_LIMITS,
            ),
        )
        return QueryRoutingResult(result=services.search.execute(request))


__all__ = ["QueryRoutingResult", "route_query_or_search"]
