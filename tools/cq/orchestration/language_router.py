"""Language routing helpers for multi-language orchestration."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.types import QueryLanguage

__all__ = ["detect_language", "route_by_language"]


def detect_language(*, target: str | None = None, lang: str = "auto") -> QueryLanguage:
    """Detect language from explicit hint or target path.

    Returns:
        QueryLanguage: Effective routing language.
    """
    if lang in {"python", "rust"}:
        return lang
    if target is not None and Path(target).suffix == ".rs":
        return "rust"
    return "python"


def route_by_language(request: object, *, lang: str = "auto") -> QueryLanguage:
    """Route a request to the appropriate language handler.

    Returns:
        QueryLanguage: Effective language route for request handling.
    """
    target = getattr(request, "target", None)
    return detect_language(target=target if isinstance(target, str) else None, lang=lang)
