"""Query/search routing helpers for CLI command boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.result_factory import build_error_result
from tools.cq.core.schema import CqResult, ms
from tools.cq.core.structs import CqStruct
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


class QueryRoutingRequestV1(CqStruct, frozen=True):
    """Typed request envelope for query/search routing."""

    query_string: str
    root: Path
    argv: list[str]
    toolchain: Toolchain | None
    services: CqRuntimeServices
    include: list[str] | None = None
    exclude: list[str] | None = None


def _contains_empty_query_assignment(query_string: str) -> bool:
    for token in query_string.split():
        key, sep, value = token.partition("=")
        if not sep or not key:
            continue
        if not value:
            return True
    return False


def route_query_or_search(request: QueryRoutingRequestV1) -> QueryRoutingResult:
    """Route one raw query string to declarative query or search fallback.

    Returns:
        QueryRoutingResult: Parsed query or search-fallback result envelope.
    """
    from tools.cq.core.types import DEFAULT_QUERY_LANGUAGE_SCOPE
    from tools.cq.query.parser import QueryParseError, has_query_tokens, parse_query
    from tools.cq.search.pipeline.smart_search import SMART_SEARCH_LIMITS

    query_string = request.query_string
    has_tokens = has_query_tokens(query_string)
    has_empty_assignment = _contains_empty_query_assignment(query_string)
    parse_error: QueryParseError | str | None = None
    try:
        parsed_query = parse_query(query_string)
    except QueryParseError as error:
        parse_error = error
    else:
        if has_tokens and has_empty_assignment:
            parse_error = "Malformed query assignment: empty value"
        else:
            return QueryRoutingResult(parsed_query=parsed_query)

    if has_tokens:
        return QueryRoutingResult(
            result=build_error_result(
                macro="q",
                root=request.root,
                argv=request.argv,
                tc=request.toolchain,
                started_ms=ms(),
                error=parse_error,
            )
        )

    resolved_toolchain = request.toolchain
    if resolved_toolchain is None:
        from tools.cq.core.toolchain import Toolchain

        resolved_toolchain = Toolchain.detect()
    request_ctx = RequestContextV1(
        root=request.root,
        argv=request.argv,
        tc=resolved_toolchain,
    )
    search_request = RequestFactory.search(
        request_ctx,
        query=query_string,
        options=SearchRequestOptionsV1(
            mode=None,
            lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
            include_globs=request.include,
            exclude_globs=request.exclude,
            include_strings=False,
            limits=SMART_SEARCH_LIMITS,
        ),
    )
    return QueryRoutingResult(result=request.services.search.execute(search_request))


__all__ = [
    "QueryRoutingRequestV1",
    "QueryRoutingResult",
    "route_query_or_search",
]
