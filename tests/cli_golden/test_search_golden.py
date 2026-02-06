"""Golden snapshot tests for cq search command output."""

from __future__ import annotations

from io import StringIO
from typing import cast

from rich.console import Console
from tools.cq.cli_app.app import app
from tools.cq.core.report import render_markdown
from tools.cq.core.schema import CqResult, RunMeta

from tests.cli_golden._support.goldens import assert_text_snapshot


def _capture_cq_help(tokens: list[str]) -> str:
    """Capture cq help output.

    Returns
    -------
    str
        Rendered help output.
    """
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    app.help_print(tokens=tokens, console=console)
    return buffer.getvalue()


def test_search_help(*, update_golden: bool) -> None:
    """Ensure search help output matches the golden snapshot."""
    assert_text_snapshot("search_help.txt", _capture_cq_help(["search"]), update=update_golden)


def _render_search_summary() -> str:
    """Render a minimal search summary to snapshot summary fields.

    Returns
    -------
    str
        Markdown summary output.
    """
    run = RunMeta(
        macro="search",
        argv=["cq", "search", "build_graph"],
        root=".",
        started_ms=0.0,
        elapsed_ms=12.0,
        toolchain={},
    )
    summary = cast(
        "dict[str, object]",
        {
            "query": "build_graph",
            "mode": "identifier",
            "lang_scope": "auto",
            "language_order": ["python", "rust"],
            "languages": {
                "python": {
                    "scanned_files": 3,
                    "scanned_files_is_estimate": False,
                    "matched_files": 2,
                    "total_matches": 4,
                    "timed_out": False,
                    "truncated": False,
                    "caps_hit": "none",
                },
                "rust": {
                    "scanned_files": 1,
                    "scanned_files_is_estimate": False,
                    "matched_files": 1,
                    "total_matches": 1,
                    "timed_out": False,
                    "truncated": False,
                    "caps_hit": "none",
                },
            },
            "cross_language_diagnostics": [],
            "include": ["src/**"],
            "exclude": ["tests/**"],
            "pattern": r"\bbuild_graph\b",
            "caps_hit": "max_total_matches",
            "scanned_files_is_estimate": False,
        },
    )
    result = CqResult(run=run, summary=summary)
    return render_markdown(result)


def test_search_summary_snapshot(*, update_golden: bool) -> None:
    """Ensure search summary output matches the golden snapshot."""
    assert_text_snapshot("search_summary.txt", _render_search_summary(), update=update_golden)
