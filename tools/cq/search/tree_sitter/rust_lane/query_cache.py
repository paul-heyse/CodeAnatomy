"""Query pack caching for Rust enrichment runtime.

This module provides cached query pack loading and compilation.
"""

from __future__ import annotations

from functools import lru_cache

from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1
from tools.cq.search.tree_sitter.core.lane_support import ENRICHMENT_ERRORS
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.planner import build_pack_plan, sort_pack_plans
from tools.cq.search.tree_sitter.rust_lane.bundle import load_rust_query_sources


@lru_cache(maxsize=1)
def _pack_source_rows() -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    """Load and compile Rust query pack sources.

    Returns:
    -------
    tuple[tuple[str, str, QueryPackPlanV1], ...]
        Tuple of (pack_name, source, plan) sorted by pack priority.
    """
    sources = load_rust_query_sources(profile_name="rust_search_enriched")
    source_rows: list[tuple[str, str, QueryPackPlanV1]] = []
    for source in sources:
        if not source.pack_name.endswith(".scm"):
            continue
        try:
            source_rows.append(
                (
                    source.pack_name,
                    source.source,
                    build_pack_plan(
                        pack_name=source.pack_name,
                        query=compile_query(
                            language="rust",
                            pack_name=source.pack_name,
                            source=source.source,
                            request_surface="artifact",
                        ),
                        query_text=source.source,
                        language="rust",
                    ),
                )
            )
        except ENRICHMENT_ERRORS:
            continue
    return tuple(
        (pack_name, source, plan) for pack_name, source, plan in sort_pack_plans(source_rows)
    )


def _pack_sources() -> tuple[tuple[str, str], ...]:
    """Return pack names and sources without plans.

    Returns:
    -------
    tuple[tuple[str, str], ...]
        Tuple of (pack_name, source) pairs.
    """
    return tuple((pack_name, source) for pack_name, source, _ in _pack_source_rows())


def clear_query_cache() -> None:
    """Clear query pack cache."""
    _pack_source_rows.cache_clear()


__all__ = [
    "clear_query_cache",
]
