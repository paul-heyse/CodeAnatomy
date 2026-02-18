"""Parser control-plane settings shared across CQ runtime surfaces."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

from tools.cq.core.structs import CqStruct

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}


def _env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    return default


class ParserControlSettingsV1(CqStruct, frozen=True):
    """Parser control-plane settings."""

    reset_before_parse: bool = False
    enable_logger: bool = False
    dot_graph_dir: str | None = None


def parser_controls_from_env() -> ParserControlSettingsV1:
    """Load parser control settings from environment flags.

    Returns:
        ParserControlSettingsV1: Parser control-plane settings from env flags.
    """
    reset_before_parse = _env_flag("CQ_TREE_SITTER_PARSER_RESET", default=False)
    enable_logger = _env_flag("CQ_TREE_SITTER_PARSER_LOGGER", default=False)
    dot_graph_dir = os.getenv("CQ_TREE_SITTER_DOT_GRAPH_DIR")
    if dot_graph_dir:
        try:
            Path(dot_graph_dir).mkdir(parents=True, exist_ok=True)
        except OSError:
            dot_graph_dir = None
    return ParserControlSettingsV1(
        reset_before_parse=reset_before_parse,
        enable_logger=enable_logger,
        dot_graph_dir=dot_graph_dir,
    )


def apply_parser_controls(parser: object, settings: ParserControlSettingsV1) -> None:
    """Apply parser controls in a fail-open manner."""
    parser_any = cast("Any", parser)
    if settings.reset_before_parse and hasattr(parser, "reset"):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            parser_any.reset()

    if settings.enable_logger and hasattr(parser, "logger"):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            parser_any.logger = lambda _msg: None

    if settings.dot_graph_dir and hasattr(parser, "print_dot_graphs"):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            parser_any.print_dot_graphs(str(Path(settings.dot_graph_dir) / "tree_sitter.dot"))


__all__ = ["ParserControlSettingsV1", "apply_parser_controls", "parser_controls_from_env"]
