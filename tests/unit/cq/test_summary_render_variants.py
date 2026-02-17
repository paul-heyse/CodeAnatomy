"""Tests for rendering mode-tagged summary variants."""

from __future__ import annotations

import msgspec
from tools.cq.core.render_summary import render_summary, summary_string
from tools.cq.core.schema import CqResult, mk_result, mk_runmeta
from tools.cq.core.summary_contract import (
    CallsSummaryV1,
    ImpactSummaryV1,
    NeighborhoodSummaryV1,
    RunSummaryV1,
    SearchSummaryV1,
)


def _result(macro: str) -> CqResult:
    run = mk_runmeta(
        macro=macro,
        argv=["cq", macro],
        root=".",
        started_ms=0.0,
        toolchain={},
        run_id="run-1",
    )
    return mk_result(run)


def test_render_summary_includes_search_variant_tag() -> None:
    """Rendered summary includes variant tag for search envelopes."""
    summary = SearchSummaryV1(query="build_graph", mode="identifier")
    lines = render_summary(summary)

    assert lines[0] == "## Summary"
    assert '"summary_variant":"search"' in lines[1]
    assert '"query":"build_graph"' in lines[1]


def test_render_summary_includes_non_search_variants() -> None:
    """Each mode variant renders without contract conversion errors."""
    summaries = (
        CallsSummaryV1(mode="macro:calls", query="foo"),
        ImpactSummaryV1(mode="impact", query="foo --param x"),
        RunSummaryV1(mode="run", query="multi-step plan (2 steps)", steps=("a", "b")),
        NeighborhoodSummaryV1(mode="neighborhood", target="foo.py:10"),
    )

    for summary in summaries:
        lines = render_summary(summary)
        assert lines[0] == "## Summary"
        assert f'"summary_variant":"{summary.summary_variant}"' in lines[1]


def test_summary_string_keeps_query_and_mode_fallbacks() -> None:
    """Summary string fallback behavior remains intact with variant summaries."""
    result = _result("run")
    result = msgspec.structs.replace(result, summary=RunSummaryV1(steps=("q_0", "search_1")))

    query_value = summary_string(result, key="query", missing_reason="missing")
    mode_value = summary_string(result, key="mode", missing_reason="missing")

    assert query_value == "`multi-step plan (2 steps)`"
    assert mode_value == "`run`"
