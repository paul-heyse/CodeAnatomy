"""Diagnostics rendering for CQ results."""

from __future__ import annotations

from tools.cq.core.summary_contract import CqSummary


def summary_with_render_enrichment_metrics(
    summary: CqSummary,
    *,
    attempted: int,
    applied: int,
    failed: int,
    skipped: int,
) -> dict[str, object]:
    """Merge render enrichment metrics into summary.

    Parameters
    ----------
    summary : CqSummary
        Original summary payload.
    attempted : int
        Number of enrichment tasks attempted.
    applied : int
        Number of enrichment tasks successfully applied.
    failed : int
        Number of enrichment tasks that failed.
    skipped : int
        Number of enrichment tasks skipped.

    Returns:
    -------
    dict[str, object]
        Summary dict with enrichment metrics.
    """
    with_metrics = dict(summary.items())
    with_metrics["render_enrichment_attempted"] = attempted
    with_metrics["render_enrichment_applied"] = applied
    with_metrics["render_enrichment_failed"] = failed
    with_metrics["render_enrichment_skipped"] = skipped
    return with_metrics


__all__ = ["summary_with_render_enrichment_metrics"]
