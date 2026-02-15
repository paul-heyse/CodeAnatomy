"""Shared tree-sitter language and parser runtime helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

if TYPE_CHECKING:
    from tree_sitter import Language, Parser


def load_language(language: str) -> Language:
    """Load a tree-sitter language or raise a runtime error.

    Raises:
        RuntimeError: If the requested language is unavailable.
    """
    resolved = load_tree_sitter_language(language)
    if resolved is None:
        msg = f"tree-sitter language unavailable: {language}"
        raise RuntimeError(msg)
    return cast("Language", resolved)


def make_parser(language: str) -> Parser:
    """Construct a parser bound to one language lane."""
    from tree_sitter import Parser

    resolved = load_language(language)
    try:
        return Parser(resolved)
    except TypeError:
        parser = Parser()
        parser.language = resolved
        return parser


__all__ = ["load_language", "make_parser"]
