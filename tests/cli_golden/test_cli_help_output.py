"""Golden snapshot tests for CLI help output."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console

from cli.app import app

if TYPE_CHECKING:
    from collections.abc import Sequence


_FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _capture_help(tokens: Sequence[str] | None) -> str:
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    app.help_print(tokens=list(tokens) if tokens is not None else [], console=console)
    return buffer.getvalue()


def _assert_matches(fixture_name: str, actual: str) -> None:
    expected = (_FIXTURES / fixture_name).read_text(encoding="utf-8")
    assert actual == expected


def test_help_root() -> None:
    """Ensure root help output matches the golden snapshot."""
    _assert_matches("help_root.txt", _capture_help(None))


def test_help_build() -> None:
    """Ensure build help output matches the golden snapshot."""
    _assert_matches("help_build.txt", _capture_help(["build"]))


def test_help_delta() -> None:
    """Ensure delta help output matches the golden snapshot."""
    _assert_matches("help_delta.txt", _capture_help(["delta"]))
