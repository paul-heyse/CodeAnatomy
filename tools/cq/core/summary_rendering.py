"""Summary rendering helpers."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.summary_contract import (
    SummaryV1,
    resolve_summary_variant_name,
    summary_for_variant,
    summary_from_mapping,
)

__all__ = ["coerce_summary", "default_summary_for_variant"]


def default_summary_for_variant(variant: str) -> SummaryV1:
    """Build a default summary for a known variant.

    Returns:
        SummaryV1: Default summary contract for the requested variant.
    """
    resolved = resolve_summary_variant_name(explicit=variant)
    return summary_for_variant(resolved)


def coerce_summary(summary: SummaryV1, updates: Mapping[str, object]) -> SummaryV1:
    """Apply mapping updates by re-coercing into summary contract.

    Returns:
        SummaryV1: Updated summary contract.
    """
    merged = {**summary.to_dict(), **dict(updates)}
    return summary_from_mapping(merged)
