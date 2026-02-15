"""Tests for lazy-loaded command registration."""

from __future__ import annotations

from io import StringIO

from rich.console import Console
from tools.cq.cli_app.app import app
from tools.cq.cli_app.commands.neighborhood import neighborhood_app
from tools.cq.cli_app.infrastructure import analysis_group


def test_lazy_command_resolution() -> None:
    """Ensure lazy command import paths resolve."""
    assert app["calls"] is not None
    assert app["q"] is not None
    assert app["search"] is not None
    assert app["run"] is not None
    assert app["chain"] is not None
    assert app["ldmd"] is not None
    assert app["artifact"] is not None
    assert app["neighborhood"] is not None
    assert app["nb"] is not None


def test_lazy_help_render() -> None:
    """Ensure help rendering works with lazy-loaded commands."""
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=100)
    app.help_print(tokens=["run"], console=console)
    assert buffer.getvalue()


def test_neighborhood_group_identity() -> None:
    """Ensure neighborhood sub-app reuses the shared analysis group."""
    group = neighborhood_app.group
    if isinstance(group, tuple):
        assert group
        assert group[0] is analysis_group
    else:
        assert group is analysis_group
