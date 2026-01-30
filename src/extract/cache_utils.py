"""Backward-compatible re-exports for extract cache helpers."""

from __future__ import annotations

from extract.infrastructure.cache_utils import (
    CACHE_VERSION,
    LOCK_EXPIRE_SECONDS,
    CacheSetOptions,
    cache_for_extract,
    cache_for_kind_optional,
    cache_get,
    cache_lock,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    evict_extract_cache,
    evict_repo_scan_cache,
    stable_cache_key,
    stable_cache_label,
)

__all__ = [
    "CACHE_VERSION",
    "LOCK_EXPIRE_SECONDS",
    "CacheSetOptions",
    "cache_for_extract",
    "cache_for_kind_optional",
    "cache_get",
    "cache_lock",
    "cache_set",
    "cache_ttl_seconds",
    "diskcache_profile_from_ctx",
    "evict_extract_cache",
    "evict_repo_scan_cache",
    "stable_cache_key",
    "stable_cache_label",
]
