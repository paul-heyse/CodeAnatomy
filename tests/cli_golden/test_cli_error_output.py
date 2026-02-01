"""Golden snapshot tests for CLI error output."""

from __future__ import annotations

from contextlib import suppress
from io import StringIO
from typing import TYPE_CHECKING

from cyclopts.exceptions import CycloptsError
from rich.console import Console

from cli.app import app
from tests.cli_golden._support.goldens import assert_text_snapshot

if TYPE_CHECKING:
    from collections.abc import Sequence


def _capture_error(tokens: Sequence[str]) -> str:
    buffer = StringIO()
    error_console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    with suppress(CycloptsError):
        app.parse_args(tokens, exit_on_error=False, print_error=True, error_console=error_console)
    return buffer.getvalue()


def test_error_unknown_command(*, update_golden: bool) -> None:
    """Ensure unknown command errors match the golden snapshot."""
    assert_text_snapshot(
        "error_unknown_command.txt", _capture_error(["unknown"]), update=update_golden
    )


def test_error_missing_repo_root(*, update_golden: bool) -> None:
    """Ensure missing argument errors match the golden snapshot."""
    assert_text_snapshot(
        "error_missing_repo_root.txt", _capture_error(["build"]), update=update_golden
    )


def test_error_restore_conflict(*, update_golden: bool) -> None:
    """Ensure mutually exclusive restore flags produce a clear error."""
    assert_text_snapshot(
        "error_restore_conflict.txt",
        _capture_error(
            [
                "delta",
                "restore",
                "--path",
                "/tmp/table",
                "--version",
                "1",
                "--timestamp",
                "2024-01-01T00:00:00Z",
            ]
        ),
        update=update_golden,
    )
