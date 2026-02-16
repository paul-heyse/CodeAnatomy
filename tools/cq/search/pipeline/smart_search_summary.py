"""Summary-building helpers for Smart Search results."""

from __future__ import annotations

from tools.cq.core.schema import Finding
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, LanguageSearchResult


def build_search_summary(
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
    enriched_matches: list[EnrichedMatch],
    *,
    python_semantic_overview: dict[str, object],
    python_semantic_telemetry: dict[str, object],
    python_semantic_diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], list[Finding]]:
    """Build full search summary and diagnostics payload.

    Returns:
        tuple[dict[str, object], list[Finding]]: Summary payload and diagnostics findings.
    """
    from tools.cq.search.pipeline.smart_search import _build_search_summary

    return _build_search_summary(
        ctx,
        partition_results,
        enriched_matches,
        python_semantic_overview=python_semantic_overview,
        python_semantic_telemetry=python_semantic_telemetry,
        python_semantic_diagnostics=python_semantic_diagnostics,
    )


def build_language_summary(*args: object, **kwargs: object) -> dict[str, object]:
    """Build canonical multi-language summary payload.

    Returns:
        dict[str, object]: Canonical multi-language summary mapping.
    """
    from tools.cq.search.pipeline.smart_search import build_summary

    return build_summary(*args, **kwargs)


__all__ = ["build_language_summary", "build_search_summary"]
