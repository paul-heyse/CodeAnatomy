"""Language routing helpers for tree-sitter runtime surfaces."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from tools.cq.core.types import QueryLanguage

__all__ = ["detect_language", "route_by_language"]


def detect_language(target: str, *, lang: str = "auto") -> QueryLanguage:
    """Detect effective language for a path or explicit hint.

    Returns:
        QueryLanguage: Effective language selection for target.
    """
    if lang in {"python", "rust"}:
        return cast("QueryLanguage", lang)
    suffix = Path(target).suffix.lower()
    if suffix == ".rs":
        return "rust"
    return "python"


def route_by_language(
    *,
    target: str,
    lang: str = "auto",
) -> QueryLanguage:
    """Route a runtime request to a concrete language lane.

    Returns:
        QueryLanguage: Effective language lane for runtime processing.
    """
    return detect_language(target, lang=lang)
