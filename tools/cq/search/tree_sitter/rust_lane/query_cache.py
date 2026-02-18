"""Query pack caching for Rust enrichment runtime.

This module provides cached query pack loading and compilation.
"""

from __future__ import annotations

from tools.cq.search.cache.registry import CACHE_REGISTRY
from tools.cq.search.tree_sitter.core.lane_support import ENRICHMENT_ERRORS
from tools.cq.search.tree_sitter.query.planner import (
    resolve_pack_source_rows_cached,
    resolve_pack_sources_cached,
)
from tools.cq.search.tree_sitter.rust_lane.bundle import load_rust_query_sources

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
    ensure_query_cache_callback_registered()
    return resolve_pack_sources_cached(
        language="rust",
        source_rows=tuple(
            (source.pack_name, source.source)
            for source in load_rust_query_sources(profile_name="rust_search_enriched")
        ),
        request_surface="artifact",
        dedupe_by_pack_name=False,
        ignored_errors=ENRICHMENT_ERRORS,
    )


def clear_query_cache() -> None:
    """Clear query pack cache."""
    resolve_pack_source_rows_cached.cache_clear()
    resolve_pack_sources_cached.cache_clear()


__all__ = [
    "clear_query_cache",
    "ensure_query_cache_callback_registered",
]
