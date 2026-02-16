"""Follow-up suggestion generation for Smart Search."""

from __future__ import annotations

from tools.cq.core.schema import Finding
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def generate_followup_suggestions(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> list[Finding]:
    """Generate actionable follow-up suggestions for Smart Search results.

    Returns:
        list[Finding]: Follow-up findings generated from current search results.
    """
    from tools.cq.search.pipeline.smart_search import build_followups

    return build_followups(matches, query, mode)


__all__ = ["generate_followup_suggestions"]
