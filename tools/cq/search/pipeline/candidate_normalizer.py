"""Candidate normalization helpers for smart-search target selection."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.definition_parser import (
    extract_definition_kind,
    extract_definition_name,
    is_definition_like_text,
)
from tools.cq.core.schema import DetailPayload, Finding

if TYPE_CHECKING:
    from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


_DEFINITION_NODE_KINDS = {
    "function_definition",
    "class_definition",
    "decorated_definition",
    "function_item",
    "struct_item",
    "enum_item",
    "trait_item",
    "impl_item",
    "mod_item",
}


def is_definition_candidate_match(match: EnrichedMatch) -> bool:
    """Return whether an enriched match is a definition candidate."""
    if is_definition_like_text(match.text):
        return True
    if match.category == "definition":
        return True
    return match.node_kind in _DEFINITION_NODE_KINDS


def definition_kind_from_text(text: str) -> str:
    """Infer target kind from definition text.

    Returns:
        Normalized target kind string.
    """
    kind = extract_definition_kind(text)
    if kind in {"function", "class", "type"}:
        return kind
    return "function"


def definition_name_from_text(text: str, *, fallback: str) -> str:
    """Extract definition symbol name from source text.

    Returns:
        Parsed symbol name when detected, otherwise fallback.
    """
    parsed = extract_definition_name(text)
    return parsed if parsed is not None else fallback


def build_definition_candidate_finding(
    match: EnrichedMatch,
    root: Path,
    *,
    build_finding_fn: Callable[[EnrichedMatch, Path], Finding],
) -> Finding | None:
    """Build normalized definition candidate finding from enriched match.

    Returns:
        Normalized finding when match is definition-like, otherwise `None`.
    """
    if not is_definition_candidate_match(match):
        return None
    finding = build_finding_fn(match, root)
    symbol = definition_name_from_text(match.text, fallback=match.match_text or "target")
    kind = definition_kind_from_text(match.text)
    if kind == "module":
        kind = "type"
    data = dict(finding.details.data)
    data["name"] = symbol
    data["kind"] = kind
    data["signature"] = match.text.strip()
    return Finding(
        category="definition",
        message=f"{kind}: {symbol}",
        anchor=finding.anchor,
        severity=finding.severity,
        details=DetailPayload(
            kind=kind,
            score=finding.details.score,
            data_items=tuple(sorted(data.items())),
        ),
    )


__all__ = [
    "build_definition_candidate_finding",
    "definition_kind_from_text",
    "definition_name_from_text",
    "is_definition_candidate_match",
    "is_definition_like_text",
]
