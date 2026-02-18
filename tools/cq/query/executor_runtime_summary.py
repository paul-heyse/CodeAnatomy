"""Typed summary-update helpers for query runtime execution paths."""

from __future__ import annotations

from tools.cq.core.schema import CqResult
from tools.cq.core.summary_types import as_search_summary
from tools.cq.core.summary_update_contracts import EntitySummaryUpdateV1


def entity_summary_updates(result: CqResult) -> EntitySummaryUpdateV1:
    """Build typed entity-query summary updates from a result summary.

    Returns:
        Typed entity summary update payload.
    """
    summary = as_search_summary(result.summary)
    return EntitySummaryUpdateV1(
        matches=summary.matches,
        total_defs=summary.total_defs,
        total_calls=summary.total_calls,
        total_imports=summary.total_imports,
    )


__all__ = ["entity_summary_updates"]
