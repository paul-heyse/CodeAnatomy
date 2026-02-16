"""Tests for parser control settings and application."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.tree_sitter.core.parser_controls import (
    ParserControlSettingsV1,
    apply_parser_controls,
    parser_controls_from_env,
)


class _Parser:
    def __init__(self) -> None:
        self.reset_calls = 0
        self.logger: object | None = None
        self.dot_path: str | None = None

    def reset(self) -> None:
        self.reset_calls += 1

    def print_dot_graphs(self, path: str) -> None:
        self.dot_path = path


def test_parser_controls_from_env_reads_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Environment flags should produce parser control settings."""
    monkeypatch.setenv("CQ_TREE_SITTER_PARSER_RESET", "1")
    monkeypatch.setenv("CQ_TREE_SITTER_PARSER_LOGGER", "1")
    monkeypatch.setenv("CQ_TREE_SITTER_DOT_GRAPH_DIR", str(tmp_path))

    settings = parser_controls_from_env()
    assert settings.reset_before_parse is True
    assert settings.enable_logger is True
    assert settings.dot_graph_dir == str(tmp_path)


def test_apply_parser_controls_sets_parser_hooks(tmp_path: Path) -> None:
    """Applying parser controls should invoke supported parser hooks."""
    parser = _Parser()
    settings = ParserControlSettingsV1(
        reset_before_parse=True,
        enable_logger=True,
        dot_graph_dir=str(tmp_path),
    )

    apply_parser_controls(parser, settings)

    assert parser.reset_calls == 1
    assert callable(parser.logger)
    assert parser.dot_path is not None
