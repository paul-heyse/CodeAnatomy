"""Tests for incremental coverage invariants and backstops."""

from __future__ import annotations

from tools.cq.search.enrichment.incremental_compound_plane import build_compound_bundle


def test_compound_plane_invariants_cover_missing_sides() -> None:
    """Track both ts-only and dis-only bindings in coverage backstops."""
    payload = build_compound_bundle(
        ts_occurrences=[{"binding_id": "scope:a", "context": "code", "node_kind": "identifier"}],
        dis_events=[{"binding_id": "scope:b", "event": "use"}],
    )
    metrics = payload.get("join_metrics")
    assert isinstance(metrics, dict)
    assert metrics.get("ts_only") == 1
    assert metrics.get("dis_only") == 1
    backstops = payload.get("coverage_backstops")
    assert isinstance(backstops, dict)
    missing_in_dis = backstops.get("missing_in_dis")
    missing_in_ts = backstops.get("missing_in_ts")
    assert isinstance(missing_in_dis, list)
    assert isinstance(missing_in_ts, list)
    assert "scope:a" in missing_in_dis
    assert "scope:b" in missing_in_ts
