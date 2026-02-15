"""Flattened diagnostics: recovery hints and diagnostic collection."""

from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from typing import TYPE_CHECKING, cast

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
)
from tools.cq.search.tree_sitter.core.infrastructure import load_language
from tools.cq.search.tree_sitter.query.support import diagnostics_query_path

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Query

try:
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterQuery = None


# -- Recovery Hints -----------------------------------------------------------


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


# -- Diagnostic Collection ----------------------------------------------------


@lru_cache(maxsize=2)
def _language(language: str) -> Language:
    return load_language(language)


@lru_cache(maxsize=2)
def _diagnostic_query(language: str) -> Query | None:
    if _TreeSitterQuery is None:
        return None
    path = diagnostics_query_path(language)
    if not path.exists():
        return None
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return _TreeSitterQuery(_language(language), source)
    except (RuntimeError, TypeError, ValueError):
        return None


def collect_tree_sitter_diagnostics(
    *,
    language: str,
    root: Node,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int = 1024,
) -> tuple[TreeSitterDiagnosticV1, ...]:
    """Collect diagnostics rows for one tree/window set.

    Returns:
        tuple[TreeSitterDiagnosticV1, ...]: A tuple of diagnostic rows.
    """
    try:
        language_obj = _language(language)
    except (RuntimeError, TypeError, ValueError):
        return ()
    query = _diagnostic_query(language)
    if query is None:
        return ()
    from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches

    matches, _telemetry = run_bounded_query_matches(
        query,
        root,
        windows=windows,
        settings=QueryExecutionSettingsV1(
            match_limit=match_limit,
            require_containment=True,
            window_mode="containment_preferred",
        ),
    )
    rows: list[TreeSitterDiagnosticV1] = []
    for _idx, capture_map in matches:
        for capture_name, nodes in capture_map.items():
            if not isinstance(capture_name, str):
                continue
            kind = "ERROR" if capture_name.endswith("error") else "MISSING"
            for node in nodes:
                start_point = getattr(node, "start_point", (0, 0))
                end_point = getattr(node, "end_point", start_point)
                rows.append(
                    TreeSitterDiagnosticV1(
                        kind=kind,
                        start_byte=int(getattr(node, "start_byte", 0)),
                        end_byte=int(getattr(node, "end_byte", 0)),
                        start_line=int(start_point[0]) + 1,
                        start_col=int(start_point[1]),
                        end_line=int(end_point[0]) + 1,
                        end_col=int(end_point[1]),
                        message=f"tree-sitter {kind}",
                        metadata={
                            "capture": capture_name,
                            "expected": list(
                                recovery_hints_for_node(
                                    language=language_obj,
                                    node=node,
                                )
                            ),
                        },
                    )
                )
    return tuple(rows)


__all__ = ["collect_tree_sitter_diagnostics", "recovery_hints_for_node"]
