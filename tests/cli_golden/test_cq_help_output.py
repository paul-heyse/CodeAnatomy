"""Golden snapshot tests for CQ help output."""

from __future__ import annotations

from io import StringIO

from rich.console import Console
from tools.cq.cli_app.app import app

from tests.cli_golden._support.goldens import assert_text_snapshot


def _capture_help(tokens: list[str] | None = None) -> str:
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    app.help_print(tokens=tokens or [], console=console)
    return buffer.getvalue()


def test_cq_help_root(*, update_golden: bool) -> None:
    """Ensure CQ root help output matches snapshot."""
    assert_text_snapshot("cq_help_root.txt", _capture_help(), update=update_golden)
