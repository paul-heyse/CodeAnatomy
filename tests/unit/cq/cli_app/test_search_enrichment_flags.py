"""Tests for CLI search enrichment flags."""

from __future__ import annotations

from tools.cq.cli_app.app import app


def test_search_cli_parses_enrichment_flags() -> None:
    """Parse search CLI enrichment flags into options payload."""
    _cmd, bound, _extra = app.parse_args(
        ["search", "build_graph", "--no-enrich", "--enrich-mode", "full"]
    )
    opts = bound.kwargs["opts"]
    assert opts.enrich is False
    assert opts.enrich_mode == "full"
