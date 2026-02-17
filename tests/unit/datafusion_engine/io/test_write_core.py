"""Tests for write-core helper behavior."""

from __future__ import annotations

from datafusion_engine.io.write_core import _replace_where_predicate


def test_replace_where_predicate_reads_both_aliases() -> None:
    """Predicate helper accepts both legacy and canonical option names."""
    assert _replace_where_predicate({"replace_where": "x = 1"}) == "x = 1"
    assert _replace_where_predicate({"predicate": "y = 2"}) == "y = 2"
