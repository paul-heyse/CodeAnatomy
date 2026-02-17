"""Tests for typed search context windows."""

from __future__ import annotations

import pytest
from tools.cq.search.pipeline.context_window import (
    ContextWindow,
    compute_search_context_window,
)

_EXPECTED_END_LINE_WITH_RADIUS = 12


def test_context_window_around_bounds_to_one() -> None:
    """around() should clamp the start line to one."""
    window = ContextWindow.around(center_line=2, radius=10)
    assert window.start_line == 1
    assert window.end_line == _EXPECTED_END_LINE_WITH_RADIUS


def test_context_window_rejects_invalid_range() -> None:
    """ContextWindow should enforce end >= start."""
    with pytest.raises(ValueError, match="Invalid context window"):
        ContextWindow(start_line=10, end_line=9)


def test_compute_search_context_window_returns_typed_window() -> None:
    """Search context computation should return ContextWindow, not dict."""
    source_lines = [
        "import os",
        "",
        "def worker():",
        "    return os.getcwd()",
    ]
    window = compute_search_context_window(source_lines, match_line=1, def_lines=[(3, 0)])
    assert isinstance(window, ContextWindow)
    assert window.start_line == 1
    assert window.end_line >= window.start_line
