"""Query pack caching for Rust enrichment runtime.

This module provides cached query pack loading and compilation.
"""

from __future__ import annotations

from functools import lru_cache

from tools.cq.search.cache.registry import CACHE_REGISTRY
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1
from tools.cq.search.tree_sitter.core.lane_support import ENRICHMENT_ERRORS
from tools.cq.search.tree_sitter.query.planner import (
    resolve_pack_source_rows_cached,
)
from tools.cq.search.tree_sitter.rust_lane.bundle import load_rust_query_sources


@lru_cache(maxsize=1)
def _pack_source_rows() -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    """Load and compile Rust query pack sources.

    Returns:
    -------
    tuple[tuple[str, str, QueryPackPlanV1], ...]
        Tuple of (pack_name, source, plan) sorted by pack priority.
    """
    ensure_query_cache_callback_registered()
    return resolve_pack_source_rows_cached(
        language="rust",
        source_rows=tuple(
            (source.pack_name, source.source)
            for source in load_rust_query_sources(profile_name="rust_search_enriched")
        ),
        request_surface="artifact",
        dedupe_by_pack_name=False,
        ignored_errors=ENRICHMENT_ERRORS,
    )


_CLEAR_CALLBACK_STATE: dict[str, bool] = {"registered": False}


def ensure_query_cache_callback_registered() -> None:
    """Lazily register query cache clear callback once."""
    if _CLEAR_CALLBACK_STATE["registered"]:
        return
    CACHE_REGISTRY.register_clear_callback("rust", clear_query_cache)
    _CLEAR_CALLBACK_STATE["registered"] = True


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
    "ensure_query_cache_callback_registered",
]
