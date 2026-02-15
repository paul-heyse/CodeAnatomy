"""Golden snapshot tests for CQ parse-error output."""

from __future__ import annotations

from contextlib import suppress
from io import StringIO

from cyclopts.exceptions import CycloptsError
from rich.console import Console
from tools.cq.cli_app.app import app

from tests.cli_golden._support.goldens import assert_text_snapshot


def _capture_error(tokens: list[str]) -> str:
    buffer = StringIO()
    error_console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    with suppress(CycloptsError):
        app.parse_args(
            tokens,
            exit_on_error=False,
            print_error=True,
            error_console=error_console,
        )
    return buffer.getvalue()


def test_cq_error_unknown_command(*, update_golden: bool) -> None:
    """Ensure unknown CQ command errors match snapshot."""
    assert_text_snapshot(
        "cq_error_unknown_command.txt",
        _capture_error(["unknown-command"]),
        update=update_golden,
    )
