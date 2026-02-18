"""Backward-compatible parser-control import surface for tree-sitter modules."""

from __future__ import annotations

from tools.cq.core.parser_controls import (
    ParserControlSettingsV1,
    apply_parser_controls,
    parser_controls_from_env,
)

__all__ = ["ParserControlSettingsV1", "apply_parser_controls", "parser_controls_from_env"]
