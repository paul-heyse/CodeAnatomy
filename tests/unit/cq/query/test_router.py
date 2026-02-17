"""Tests for query/search routing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.bootstrap import CqRuntimeServices, resolve_runtime_services
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.services import SearchService, SearchServiceRequest
from tools.cq.query.router import QueryRoutingRequestV1, route_query_or_search


@dataclass
class _FakeSearchService:
    calls: int = 0

    def execute(self, _request: SearchServiceRequest) -> CqResult:
        self.calls += 1
        return CqResult(
            run=RunMeta(
                macro="search",
                argv=["cq", "search", "x"],
                root=".",
                started_ms=0.0,
                elapsed_ms=0.0,
                toolchain={},
            )
        )


def _build_services() -> tuple[CqRuntimeServices, _FakeSearchService]:
    """Build a runtime service bundle with a typed search spy.

    Returns:
        tuple[CqRuntimeServices, _FakeSearchService]: Runtime service bundle and
            search-call spy.
    """
    base = resolve_runtime_services(Path())
    spy = _FakeSearchService()
    return (
        CqRuntimeServices(
            search=SearchService(execute_fn=spy.execute),
            entity=base.entity,
            calls=base.calls,
            cache=base.cache,
            policy=base.policy,
        ),
        spy,
    )


def test_route_query_or_search_returns_parsed_query_for_valid_query() -> None:
    """Route valid query syntax to the parsed-query path."""
    services, spy = _build_services()

    routing = route_query_or_search(
        QueryRoutingRequestV1(
            query_string="entity=function name=build_graph",
            root=Path(),
            argv=["cq", "q", "entity=function name=build_graph"],
            toolchain=None,
            services=services,
        )
    )

    assert routing.parsed_query is not None
    assert routing.result is None
    assert spy.calls == 0


def test_route_query_or_search_falls_back_to_search_for_plain_text() -> None:
    """Route plain text to search fallback path."""
    services, spy = _build_services()

    routing = route_query_or_search(
        QueryRoutingRequestV1(
            query_string="build_graph",
            root=Path(),
            argv=["cq", "q", "build_graph"],
            toolchain=None,
            services=services,
        )
    )

    assert routing.parsed_query is None
    assert routing.result is not None
    assert routing.result.run.macro == "search"
    assert spy.calls == 1


def test_route_query_or_search_returns_error_result_for_query_parse_failure() -> None:
    """Return error result for malformed query syntax that has query tokens."""
    services, spy = _build_services()

    routing = route_query_or_search(
        QueryRoutingRequestV1(
            query_string="entity=function name=",
            root=Path(),
            argv=["cq", "q", "entity=function name="],
            toolchain=None,
            services=services,
        )
    )

    assert routing.parsed_query is None
    assert routing.result is not None
    assert routing.result.run.macro == "q"
    assert routing.result.summary.error is not None
    assert spy.calls == 0
