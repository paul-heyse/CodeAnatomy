"""Parse-state recovery hints via `next_state` + `lookahead_iterator`."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from tree_sitter import Language, Node


def recovery_hints_for_node(
    *,
    language: Language,
    node: Node,
    max_expected: int = 12,
) -> tuple[str, ...]:
    """Return expected node kinds near parse failure boundaries."""
    if not hasattr(language, "next_state") or not hasattr(language, "lookahead_iterator"):
        return ()
    state_info = _parse_state(node)
    if state_info is None:
        return ()
    parse_state, grammar_id = state_info
    try:
        next_state = language.next_state(parse_state, grammar_id)
        lookahead = language.lookahead_iterator(next_state)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return ()
    if not isinstance(lookahead, Iterable):
        return ()
    lookahead_items = cast("Iterable[object]", lookahead)

    expected: list[str] = []
    for symbol_id in _iter_symbol_ids(lookahead_items):
        symbol_name = _symbol_name(language=language, symbol_id=symbol_id)
        if symbol_name is None:
            continue
        expected.append(symbol_name)
        if len(expected) >= max_expected:
            break
    return tuple(expected)


def _parse_state(node: Node) -> tuple[int, int] | None:
    parse_state = getattr(node, "parse_state", None)
    grammar_id = getattr(node, "grammar_id", None)
    if not isinstance(parse_state, int) or not isinstance(grammar_id, int):
        return None
    return parse_state, grammar_id


def _iter_symbol_ids(lookahead: Iterable[object]) -> tuple[int, ...]:
    symbol_ids: list[int] = []
    for entry in lookahead:
        if isinstance(entry, tuple):
            if not entry:
                continue
            symbol_id = entry[0]
        else:
            symbol_id = entry
        if isinstance(symbol_id, int):
            symbol_ids.append(symbol_id)
    return tuple(symbol_ids)


def _symbol_name(*, language: Language, symbol_id: int) -> str | None:
    try:
        symbol_name = language.node_kind_for_id(symbol_id)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None
    if not isinstance(symbol_name, str) or not symbol_name:
        return None
    return symbol_name


__all__ = ["recovery_hints_for_node"]
