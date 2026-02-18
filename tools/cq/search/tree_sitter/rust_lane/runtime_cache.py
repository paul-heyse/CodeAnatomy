"""Cache and parser lifecycle management for Rust tree-sitter runtime.

This module handles parse-tree caching, session management, and parser
creation for Rust enrichment operations.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, cast

from tools.cq.search._shared.bounded_cache import BoundedCache
from tools.cq.search.cache.registry import CACHE_REGISTRY
from tools.cq.search.tree_sitter.core.infrastructure import cached_field_ids
from tools.cq.search.tree_sitter.core.lane_support import make_parser_from_language
from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language
from tools.cq.search.tree_sitter.core.parse import clear_parse_session, get_parse_session
from tools.cq.search.tree_sitter.core.runtime_context import get_default_context
from tools.cq.search.tree_sitter.query.compiler import compile_query

if TYPE_CHECKING:
    from tree_sitter import Language, Tree

    from tools.cq.search.tree_sitter.core.parse import ParseSession

try:
    from tree_sitter import Parser as _TreeSitterParser
except ImportError:  # pragma: no cover - exercised via availability checks
    _TreeSitterParser = None


_MAX_TREE_CACHE_ENTRIES = 128
_CACHE_REGISTRATION_STATE: dict[str, bool] = {"registered": False}
_CLEAR_CALLBACK_STATE: dict[str, bool] = {"registered": False}


def _tree_cache() -> BoundedCache[str, None]:
    ensure_runtime_cache_callback_registered()
    runtime_context = get_default_context()
    cache = runtime_context.rust_tree_cache
    if isinstance(cache, BoundedCache):
        return cache
    created: BoundedCache[str, None] = BoundedCache(
        max_size=_MAX_TREE_CACHE_ENTRIES,
        policy="lru",
    )
    runtime_context.rust_tree_cache = created
    if not _CACHE_REGISTRATION_STATE["registered"]:
        CACHE_REGISTRY.register_cache("rust", "rust_lane:tree_cache", created)
        _CACHE_REGISTRATION_STATE["registered"] = True
    return created


def ensure_runtime_cache_callback_registered() -> None:
    """Lazily register Rust runtime cache clear callback once."""
    if _CLEAR_CALLBACK_STATE["registered"]:
        return
    CACHE_REGISTRY.register_clear_callback("rust", clear_tree_sitter_rust_cache)
    _CLEAR_CALLBACK_STATE["registered"] = True


def _tree_cache_evictions() -> int:
    return int(get_default_context().rust_tree_cache_evictions)


def _set_tree_cache_evictions(value: int) -> None:
    get_default_context().rust_tree_cache_evictions = max(0, int(value))


@lru_cache(maxsize=1)
def _rust_language() -> Language:
    """Return the cached Rust language object.

    Returns:
    -------
    Language
        The tree-sitter Rust language binding.

    Raises:
        RuntimeError: When Rust language bindings are unavailable.
    """
    resolved = load_tree_sitter_language("rust")
    if resolved is None:
        msg = "tree_sitter_rust language bindings are unavailable"
        raise RuntimeError(msg)
    return cast("Language", resolved)


@lru_cache(maxsize=1)
def _rust_field_ids() -> dict[str, int]:
    """Return field name to ID mapping for Rust grammar.

    Returns:
    -------
    dict[str, int]
        Field name to field ID mapping.
    """
    return cached_field_ids("rust")


def rust_field_ids() -> dict[str, int]:
    """Return cached Rust grammar field IDs.

    Returns:
    -------
    dict[str, int]
        Field name to field ID mapping.
    """
    return _rust_field_ids()


def _touch_tree_cache(_session: object, cache_key: str | None) -> None:
    """Touch cache entry to update LRU ordering.

    Parameters
    ----------
    _session
        The parse session (unused, retained for signature compatibility).
    cache_key
        The cache key to touch.
    """
    if not cache_key:
        return
    cache = _tree_cache()
    if cache_key in cache:
        cache.put(cache_key, None)
        return
    at_capacity = len(cache) >= _MAX_TREE_CACHE_ENTRIES
    cache.put(cache_key, None)
    if at_capacity:
        _set_tree_cache_evictions(_tree_cache_evictions() + 1)


def _parse_with_session(
    source: str,
    *,
    cache_key: str | None,
    parse_session: ParseSession | None = None,
) -> tuple[Tree | None, bytes, tuple[object, ...]]:
    """Parse Rust source using cached session and parser.

    Parameters
    ----------
    source
        Full source text.
    cache_key
        Stable file key for cache reuse.

    Returns:
    -------
    tuple[Tree | None, bytes, tuple[object, ...]]
        Parsed tree, source bytes, and changed ranges.
    """
    source_bytes = source.encode("utf-8", errors="replace")
    if _TreeSitterParser is None:
        return None, source_bytes, ()
    session = parse_session or get_parse_session(
        language="rust", parser_factory=lambda: make_parser_from_language(_rust_language())
    )
    _touch_tree_cache(_session=session, cache_key=cache_key)
    tree, changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, source_bytes, tuple(changed_ranges) if changed_ranges is not None else ()


def clear_tree_sitter_rust_cache() -> None:
    """Clear per-process Rust parser caches and reset debug counters."""
    from tools.cq.search.tree_sitter.rust_lane.query_cache import (
        clear_query_cache,
        ensure_query_cache_callback_registered,
    )

    clear_parse_session(language="rust")
    _tree_cache().clear()
    _set_tree_cache_evictions(0)
    _rust_language.cache_clear()
    compile_query.cache_clear()
    ensure_query_cache_callback_registered()
    clear_query_cache()


def get_tree_sitter_rust_cache_stats() -> dict[str, int]:
    """Return cache counters for observability/debugging.

    Returns:
    -------
    dict[str, int]
        Cache statistics (entries, hits, misses, evictions, parse counts).
    """
    session = get_parse_session(
        language="rust", parser_factory=lambda: make_parser_from_language(_rust_language())
    )
    ensure_runtime_cache_callback_registered()
    stats = session.stats()
    return {
        "entries": stats.entries,
        "cache_hits": stats.cache_hits,
        "cache_misses": stats.cache_misses,
        "cache_evictions": _tree_cache_evictions(),
        "parse_count": stats.parse_count,
        "reparse_count": stats.reparse_count,
        "edit_failures": stats.edit_failures,
    }


__all__ = [
    "clear_tree_sitter_rust_cache",
    "ensure_runtime_cache_callback_registered",
    "get_tree_sitter_rust_cache_stats",
    "rust_field_ids",
]
