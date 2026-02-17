"""Python lane enrichment dispatch wrappers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.python_lane.runtime import enrich_python_context_by_byte_range

__all__ = ["dispatch_python_enrichment"]


def dispatch_python_enrichment(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
) -> dict[str, object] | None:
    """Dispatch to Python lane byte-range enrichment.

    Returns:
        dict[str, object] | None: Enrichment payload, or ``None`` when unavailable.
    """
    return enrich_python_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
    )
