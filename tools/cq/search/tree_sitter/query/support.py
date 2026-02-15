"""Merged query utility helpers: resource paths, pack metadata, cache adapter."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from diskcache import FanoutCache
    from tree_sitter import Node, Query


# -- Resource Paths -----------------------------------------------------------

_QUERY_ROOT = Path(__file__).resolve().parents[2] / "queries"


def query_pack_dir(language: str) -> Path:
    """Return the query pack directory for one language lane."""
    return _QUERY_ROOT / language


def query_pack_path(language: str, pack_name: str) -> Path:
    """Return the absolute path for one query pack source file."""
    return query_pack_dir(language) / pack_name


def query_contracts_path(language: str) -> Path:
    """Return the absolute path for one query contracts YAML file."""
    return query_pack_dir(language) / "contracts.yaml"


def diagnostics_query_path(language: str) -> Path:
    """Return the absolute path for one diagnostics query pack file."""
    return query_pack_dir(language) / "95_diagnostics.scm"


# -- Pack Metadata ------------------------------------------------------------


def pattern_settings(query: Query, pattern_idx: int) -> dict[str, str]:
    """Return normalized string-only metadata for one pattern index."""
    settings = query.pattern_settings(pattern_idx)
    if not isinstance(settings, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in settings.items():
        if not isinstance(key, str):
            continue
        if value is None:
            continue
        out[key] = str(value)
    return out


def first_capture(capture_map: dict[str, list[Node]], capture_name: str) -> Node | None:
    """Return first node for capture name when available."""
    nodes = capture_map.get(capture_name)
    if not isinstance(nodes, list) or not nodes:
        return None
    return nodes[0]


# -- Cache Adapter ------------------------------------------------------------


def query_registry_cache(*, root: Path | None = None) -> FanoutCache | None:
    """Return a cache object for query registry stampede guards.

    The cache backend import remains lazy to avoid import-time cycles between
    query modules and cache bootstrap modules.
    """
    from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend

    backend = get_cq_cache_backend(root=root or Path.cwd())
    cache = getattr(backend, "cache", None)
    if cache is None:
        return None
    try:
        _ = cache.get
        _ = cache.set
    except AttributeError:
        return None
    return cache


__all__ = [
    "diagnostics_query_path",
    "first_capture",
    "pattern_settings",
    "query_contracts_path",
    "query_pack_dir",
    "query_pack_path",
    "query_registry_cache",
]
