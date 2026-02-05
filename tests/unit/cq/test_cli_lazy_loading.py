"""Tests for lazy-loaded command registration."""

from __future__ import annotations

from tools.cq.cli_app.app import app


def test_lazy_command_resolution() -> None:
    """Ensure lazy command import paths resolve."""
    assert app["calls"] is not None
    assert app["q"] is not None
    assert app["search"] is not None
    assert app["run"] is not None
    assert app["chain"] is not None
