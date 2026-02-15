"""Tests for parser operational control helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.tree_sitter.core.infrastructure import (
    ParserControlSettingsV1,
    apply_parser_controls,
)


class _Parser:
    def __init__(self) -> None:
        self.reset_calls = 0
        self.dot_paths: list[str] = []
        self.logger = None

    def reset(self) -> None:
        self.reset_calls += 1

    def print_dot_graphs(self, path: str) -> None:
        self.dot_paths.append(path)


def test_apply_parser_controls_respects_flags(tmp_path: Path) -> None:
    parser = _Parser()
    settings = ParserControlSettingsV1(
        reset_before_parse=True,
        enable_logger=True,
        dot_graph_dir=str(tmp_path),
    )
    apply_parser_controls(parser, settings)
    assert parser.reset_calls == 1
    assert parser.logger is not None
    assert parser.dot_paths
