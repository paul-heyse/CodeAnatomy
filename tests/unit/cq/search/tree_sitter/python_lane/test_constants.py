"""Tests for python lane shared constants."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.python_lane import constants as python_constants

EXPECTED_MAX_CAPTURE_ITEMS = 8
EXPECTED_DEFAULT_MATCH_LIMIT = 4096


def test_python_lane_constants_expected_values() -> None:
    """Python lane constants should match expected operational defaults."""
    assert python_constants.MAX_CAPTURE_ITEMS == EXPECTED_MAX_CAPTURE_ITEMS
    assert python_constants.DEFAULT_MATCH_LIMIT == EXPECTED_DEFAULT_MATCH_LIMIT
    assert "module" in python_constants.STOP_CONTEXT_KINDS
    assert "function_definition" in python_constants.PYTHON_LIFT_ANCHOR_TYPES


def test_get_python_field_ids_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """`get_python_field_ids` should delegate once and then serve from cache."""
    calls: list[str] = []

    def fake_cached_field_ids(language: str) -> dict[str, int]:
        calls.append(language)
        return {"name": 1}

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.core.infrastructure.cached_field_ids",
        fake_cached_field_ids,
    )
    python_constants.get_python_field_ids.cache_clear()

    assert python_constants.get_python_field_ids() == {"name": 1}
    assert python_constants.get_python_field_ids() == {"name": 1}
    assert calls == ["python"]
