"""Highlights query-pack execution helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core.language_registry import load_language_registry
from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.registry import load_distribution_query_source

if TYPE_CHECKING:
    from tree_sitter import Node


class HighlightTokenV1(CqStruct, frozen=True):
    """One highlights query capture normalized to token metadata."""

    capture_name: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int


class HighlightsResultV1(CqStruct, frozen=True):
    """Highlights query execution output."""

    language: str
    token_count: int = 0
    tokens: tuple[HighlightTokenV1, ...] = ()
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None


_POINT_ROW_INDEX = 0
_POINT_COL_INDEX = 1
_POINT_DIMENSIONS = 2


def _tokens_from_matches(
    matches: Sequence[tuple[int, dict[str, list[Node]]]],
) -> tuple[HighlightTokenV1, ...]:
    rows: list[HighlightTokenV1] = []
    for _pattern_idx, capture_map in matches:
        for capture_name, nodes in capture_map.items():
            normalized_name = capture_name.removeprefix("@")
            for node in nodes:
                start_point = getattr(node, "start_point", (0, 0))
                start_line = int(start_point[_POINT_ROW_INDEX]) if len(start_point) >= 1 else 0
                start_col = (
                    int(start_point[_POINT_COL_INDEX])
                    if len(start_point) >= _POINT_DIMENSIONS
                    else 0
                )
                rows.append(
                    HighlightTokenV1(
                        capture_name=normalized_name,
                        start_byte=int(getattr(node, "start_byte", 0)),
                        end_byte=int(getattr(node, "end_byte", 0)),
                        start_line=start_line,
                        start_col=start_col,
                    )
                )
    rows.sort(key=lambda row: (row.start_byte, row.end_byte, row.capture_name))
    return tuple(rows)


def run_highlights_pack(
    *,
    language: str,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1 | None = None,
) -> HighlightsResultV1:
    """Execute distribution `highlights.scm` and return normalized tokens.

    Returns:
        HighlightsResultV1: Function return value.
    """
    normalized = language.strip().lower()
    registry = load_language_registry(normalized)
    highlights_source = load_distribution_query_source(normalized, "highlights.scm")
    if not highlights_source:
        return HighlightsResultV1(
            language=normalized,
            grammar_name=registry.grammar_name if registry is not None else None,
            semantic_version=registry.semantic_version if registry is not None else None,
            abi_version=registry.abi_version if registry is not None else None,
        )
    try:
        query = compile_query(
            language=normalized,
            pack_name="highlights.scm",
            source=highlights_source,
            request_surface="distribution",
        )
        matches, _telemetry = run_bounded_query_matches(
            query,
            root,
            windows=windows,
            settings=settings,
        )
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return HighlightsResultV1(
            language=normalized,
            grammar_name=registry.grammar_name if registry is not None else None,
            semantic_version=registry.semantic_version if registry is not None else None,
            abi_version=registry.abi_version if registry is not None else None,
        )
    _ = source_bytes
    tokens = _tokens_from_matches(matches)
    return HighlightsResultV1(
        language=normalized,
        token_count=len(tokens),
        tokens=tokens,
        grammar_name=registry.grammar_name if registry is not None else None,
        semantic_version=registry.semantic_version if registry is not None else None,
        abi_version=registry.abi_version if registry is not None else None,
    )


__all__ = ["HighlightTokenV1", "HighlightsResultV1", "run_highlights_pack"]
