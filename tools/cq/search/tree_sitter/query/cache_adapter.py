"""Lazy cache adapter for tree-sitter query registry usage."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from diskcache import FanoutCache


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


__all__ = ["query_registry_cache"]
