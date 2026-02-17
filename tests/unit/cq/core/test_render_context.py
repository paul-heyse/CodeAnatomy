"""Tests for render context construction."""

from __future__ import annotations

from tools.cq.core.render_context import RenderContext


def test_render_context_minimal_builds() -> None:
    """Verify minimal render context construction succeeds."""
    context = RenderContext.minimal()
    assert isinstance(context, RenderContext)
