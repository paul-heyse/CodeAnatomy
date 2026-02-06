"""Golden snapshot tests for cq run command output."""

from __future__ import annotations

from io import StringIO

from rich.console import Console
from tools.cq.cli_app.app import app

from tests.cli_golden._support.goldens import assert_text_snapshot


def _capture_cq_help(tokens: list[str]) -> str:
    """Capture cq help output.

    Returns:
    -------
    str
        Rendered help output.
    """
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    app.help_print(tokens=tokens, console=console)
    return buffer.getvalue()


def test_run_help(*, update_golden: bool) -> None:
    """Ensure run help output matches the golden snapshot."""
    assert_text_snapshot("run_help.txt", _capture_cq_help(["run"]), update=update_golden)
