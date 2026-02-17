"""Tests for query/search routing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.query.router import route_query_or_search


@dataclass
class _FakeSearchService:
    calls: int = 0

    def execute(self, _request: object) -> CqResult:
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


@dataclass
class _FakeServices:
    search: _FakeSearchService


def test_route_query_or_search_returns_parsed_query_for_valid_query() -> None:
    """Route valid query syntax to the parsed-query path."""
    services = _FakeServices(search=_FakeSearchService())

    routing = route_query_or_search(
        "entity=function name=build_graph",
        root=Path(),
        argv=["cq", "q", "entity=function name=build_graph"],
        toolchain=None,
        services=services,  # type: ignore[arg-type]
    )

    assert routing.parsed_query is not None
    assert routing.result is None
    assert services.search.calls == 0


def test_route_query_or_search_falls_back_to_search_for_plain_text() -> None:
    """Route plain text to search fallback path."""
    services = _FakeServices(search=_FakeSearchService())

    routing = route_query_or_search(
        "build_graph",
        root=Path(),
        argv=["cq", "q", "build_graph"],
        toolchain=None,
        services=services,  # type: ignore[arg-type]
    )

    assert routing.parsed_query is None
    assert routing.result is not None
    assert routing.result.run.macro == "search"
    assert services.search.calls == 1


def test_route_query_or_search_returns_error_result_for_query_parse_failure() -> None:
    """Return error result for malformed query syntax that has query tokens."""
    services = _FakeServices(search=_FakeSearchService())

    routing = route_query_or_search(
        "entity=function name=",
        root=Path(),
        argv=["cq", "q", "entity=function name="],
        toolchain=None,
        services=services,  # type: ignore[arg-type]
    )

    assert routing.parsed_query is None
    assert routing.result is not None
    assert routing.result.run.macro == "q"
    assert routing.result.summary.error is not None
    assert services.search.calls == 0
