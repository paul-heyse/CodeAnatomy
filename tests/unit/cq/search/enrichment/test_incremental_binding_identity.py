"""Tests for incremental binding identity joins."""

from __future__ import annotations

from tools.cq.search.enrichment.incremental_compound_plane import build_binding_join


def test_binding_identity_join_matches_shared_binding_id() -> None:
    """Join TS and dis events by shared binding identity."""
    binding_id = "10:x"
    joined = build_binding_join(
        ts_occurrences=[{"binding_id": binding_id}],
        dis_events=[{"binding_id": binding_id, "event": "def"}],
    )
    binding_join = joined.get("binding_join")
    assert isinstance(binding_join, dict)
    row = binding_join.get(binding_id)
    assert isinstance(row, dict)
    assert row.get("ts") == 1
    assert row.get("dis_defs") == 1
