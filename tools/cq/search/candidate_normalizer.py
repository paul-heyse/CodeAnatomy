"""Candidate normalization helpers for smart-search target selection."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import DetailPayload, Finding

if TYPE_CHECKING:
    from tools.cq.search.smart_search import EnrichedMatch


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


def is_definition_like_text(text: str) -> bool:
    """Return whether source line text appears to be a definition."""
    stripped = text.lstrip()
    return stripped.startswith(
        (
            "def ",
            "async def ",
            "class ",
            "fn ",
            "pub fn ",
            "struct ",
            "enum ",
            "trait ",
        )
    )


def is_definition_candidate_match(match: EnrichedMatch) -> bool:
    """Return whether an enriched match is a definition candidate."""
    return is_definition_like_text(match.text) or match.node_kind in _DEFINITION_NODE_KINDS


def definition_kind_from_text(text: str) -> str:
    """Infer target kind from definition text.

    Returns:
        Normalized target kind string.
    """
    trimmed = text.lstrip()
    if trimmed.startswith("class "):
        return "class"
    if trimmed.startswith(("struct ", "enum ", "trait ", "impl ")):
        return "type"
    return "function"


def definition_name_from_text(text: str, *, fallback: str) -> str:
    """Extract definition symbol name from source text.

    Returns:
        Parsed symbol name when detected, otherwise fallback.
    """
    match = re.search(
        r"(?:async\\s+def|def|class|pub\\s+fn|fn|struct|enum|trait|impl)\\s+([A-Za-z_][A-Za-z0-9_]*)",
        text,
    )
    if match is None:
        return fallback
    return match.group(1)


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
    data = dict(finding.details.data)
    data["name"] = symbol
    data["kind"] = kind
    data["signature"] = match.text.strip()
    return Finding(
        category="definition",
        message=f"{kind}: {symbol}",
        anchor=finding.anchor,
        severity=finding.severity,
        details=DetailPayload(kind=kind, score=finding.details.score, data=data),
    )


__all__ = [
    "build_definition_candidate_finding",
    "definition_kind_from_text",
    "definition_name_from_text",
    "is_definition_candidate_match",
    "is_definition_like_text",
]
