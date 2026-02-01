"""Golden snapshot tests for CLI help output."""

from __future__ import annotations

from io import StringIO
from typing import TYPE_CHECKING

from rich.console import Console

from cli.app import app
from tests.cli_golden._support.goldens import assert_text_snapshot

if TYPE_CHECKING:
    from collections.abc import Sequence


def _capture_help(tokens: Sequence[str] | None) -> str:
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    app.help_print(tokens=list(tokens) if tokens is not None else [], console=console)
    return buffer.getvalue()


def test_help_root(*, update_golden: bool) -> None:
    """Ensure root help output matches the golden snapshot."""
    assert_text_snapshot("help_root.txt", _capture_help(None), update=update_golden)


def test_help_build(*, update_golden: bool) -> None:
    """Ensure build help output matches the golden snapshot."""
    assert_text_snapshot("help_build.txt", _capture_help(["build"]), update=update_golden)


def test_help_delta(*, update_golden: bool) -> None:
    """Ensure delta help output matches the golden snapshot."""
    assert_text_snapshot("help_delta.txt", _capture_help(["delta"]), update=update_golden)


def test_help_plan(*, update_golden: bool) -> None:
    """Ensure plan help output matches the golden snapshot."""
    assert_text_snapshot("help_plan.txt", _capture_help(["plan"]), update=update_golden)


def test_help_config(*, update_golden: bool) -> None:
    """Ensure config help output matches the golden snapshot."""
    assert_text_snapshot("help_config.txt", _capture_help(["config"]), update=update_golden)


def test_help_diag(*, update_golden: bool) -> None:
    """Ensure diag help output matches the golden snapshot."""
    assert_text_snapshot("help_diag.txt", _capture_help(["diag"]), update=update_golden)
