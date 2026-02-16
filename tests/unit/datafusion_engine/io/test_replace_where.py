# ruff: noqa: D103, INP001
"""Tests for predicate-based replace_where support."""

from __future__ import annotations

from datafusion_engine.io.write_core import _replace_where_predicate


def test_replace_where_prefers_explicit_option() -> None:
    assert _replace_where_predicate({"replace_where": "id = 1", "predicate": "id = 2"}) == "id = 1"
