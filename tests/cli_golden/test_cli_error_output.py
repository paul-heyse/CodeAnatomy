"""Golden snapshot tests for CLI error output."""

from __future__ import annotations

from contextlib import suppress
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

from cyclopts.exceptions import CycloptsError
from rich.console import Console

from cli.app import app

if TYPE_CHECKING:
    from collections.abc import Sequence


_FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _capture_error(tokens: Sequence[str]) -> str:
    buffer = StringIO()
    error_console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    with suppress(CycloptsError):
        app.parse_args(tokens, exit_on_error=False, print_error=True, error_console=error_console)
    return buffer.getvalue()


def _assert_matches(fixture_name: str, actual: str) -> None:
    expected = (_FIXTURES / fixture_name).read_text(encoding="utf-8")
    assert actual == expected


def test_error_unknown_command() -> None:
    """Ensure unknown command errors match the golden snapshot."""
    _assert_matches("error_unknown_command.txt", _capture_error(["unknown"]))


def test_error_missing_repo_root() -> None:
    """Ensure missing argument errors match the golden snapshot."""
    _assert_matches("error_missing_repo_root.txt", _capture_error(["build"]))
