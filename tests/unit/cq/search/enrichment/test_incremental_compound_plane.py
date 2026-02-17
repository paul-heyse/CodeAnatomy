"""Tests for incremental compound plane."""

from __future__ import annotations

from tools.cq.search.enrichment.incremental_compound_plane import (
    build_binding_join,
    build_compound_bundle,
)

_EXPECTED_TS_COUNT = 2
_MIN_EXPECTED_BINDINGS = 2


def test_build_binding_join_counts_ts_and_dis() -> None:
    """Count tree-sitter and dis events per binding identifier."""
    joined = build_binding_join(
        [{"binding_id": "S:foo"}, {"binding_id": "S:foo"}],
        [{"binding_id": "S:foo", "event": "def"}],
    )
    binding_join = joined.get("binding_join")
    assert isinstance(binding_join, dict)
    row = binding_join.get("S:foo")
    assert isinstance(row, dict)
    assert row.get("ts") == _EXPECTED_TS_COUNT
    assert row.get("dis_defs") == 1


def test_compound_bundle_coverage_backstops() -> None:
    """Report coverage backstops when TS and dis sets diverge."""
    bundle = build_compound_bundle(
        ts_occurrences=[{"binding_id": "S:foo", "node_kind": "call", "context": "code"}],
        dis_events=[{"binding_id": "S:bar", "event": "use"}],
    )
    metrics = bundle.get("join_metrics")
    assert isinstance(metrics, dict)
    bindings_total = metrics.get("bindings_total")
    assert isinstance(bindings_total, int)
    assert bindings_total >= _MIN_EXPECTED_BINDINGS
    assert isinstance(bundle.get("coverage_backstops"), dict)
