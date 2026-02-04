"""DiskCache helpers for extract workflows."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from diskcache import Lock

from cache.diskcache_factory import (
    DiskCacheKind,
    DiskCacheProfile,
    cache_for_kind,
    evict_cache_tag,
)
from serde_msgspec import to_builtins
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


CACHE_VERSION: int = 1
LOCK_EXPIRE_SECONDS: float = 60.0


def stable_cache_key(prefix: str, payload: Mapping[str, object]) -> str:
    """Return a stable string key for DiskCache usage.

    Returns
    -------
    str
        Stable cache key string.
    """
    digest = hash_msgpack_canonical(to_builtins({"version": CACHE_VERSION, **payload}))
    return f"{prefix}:{digest}"


def stable_cache_label(prefix: str, payload: Mapping[str, object]) -> str:
    """Return a stable label usable for cache directories or queue names.

    Returns
    -------
    str
        Stable label derived from the payload fingerprint.
    """
    digest = hash_msgpack_canonical(to_builtins({"version": CACHE_VERSION, **payload}))
    return f"{prefix}_{digest}"


def diskcache_profile_from_ctx(
    profile: DataFusionRuntimeProfile | None,
) -> DiskCacheProfile | None:
    """Return the DiskCache profile for a runtime profile.

    Returns
    -------
    DiskCacheProfile | None
        DiskCache profile when configured.
    """
    if profile is None:
        return None
    return profile.policies.diskcache_profile


def cache_for_extract(
    profile: DiskCacheProfile | None,
) -> Cache | FanoutCache | None:
    """Return the DiskCache instance for extract workloads.

    Returns
    -------
    Cache | FanoutCache | None
        Cache instance for extract workloads.
    """
    if profile is None:
        return None
    return cache_for_kind(profile, "extract")


def cache_for_kind_optional(
    profile: DiskCacheProfile | None,
    kind: DiskCacheKind,
) -> Cache | FanoutCache | None:
    """Return a cache for the requested kind when configured.

    Returns
    -------
    Cache | FanoutCache | None
        Cache instance for the requested kind.
    """
    if profile is None:
        return None
    return cache_for_kind(profile, kind)


def cache_get(
    cache: Cache | FanoutCache | None,
    *,
    key: str,
    default: object | None = None,
) -> object | None:
    """Return a cache entry with retry enabled.

    Returns
    -------
    object | None
        Cached value or default when missing.
    """
    if cache is None:
        return default
    return cache.get(key, default=default, retry=True)


def cache_set(
    cache: Cache | FanoutCache | None,
    *,
    key: str,
    value: object,
    options: CacheSetOptions | None = None,
) -> bool:
    """Set a cache entry with retry enabled.

    Returns
    -------
    bool
        ``True`` when the entry was stored.
    """
    if cache is None:
        return False
    cache_options = options or CacheSetOptions()
    return bool(
        cache.set(
            key,
            value,
            expire=cache_options.expire,
            tag=cache_options.tag,
            read=cache_options.read,
            retry=True,
        )
    )


@dataclass(frozen=True)
class CacheSetOptions:
    """Options for storing a value in DiskCache."""

    expire: float | None = None
    tag: str | None = None
    read: bool = False


def cache_ttl_seconds(profile: DiskCacheProfile | None, kind: DiskCacheKind) -> float | None:
    """Return the TTL in seconds for a cache kind when configured.

    Returns
    -------
    float | None
        TTL in seconds when configured.
    """
    if profile is None:
        return None
    return profile.ttl_for(kind)


def evict_extract_cache(profile: DiskCacheProfile | None, *, tag: str) -> int:
    """Evict extract cache entries for a tag.

    Returns
    -------
    int
        Count of evicted entries.
    """
    if profile is None:
        return 0
    return evict_cache_tag(profile, kind="extract", tag=tag)


def evict_repo_scan_cache(profile: DiskCacheProfile | None, *, tag: str) -> int:
    """Evict repo scan cache entries for a tag.

    Returns
    -------
    int
        Count of evicted entries.
    """
    if profile is None:
        return 0
    return evict_cache_tag(profile, kind="repo_scan", tag=tag)


@contextmanager
def cache_lock(
    cache: Cache | FanoutCache | None,
    *,
    key: str,
    expire_seconds: float | None = None,
) -> Iterator[None]:
    """Return a cross-process lock for cache stampede protection.

    Yields
    ------
    None
        Context manager scope for the cache lock.
    """
    if cache is None:
        yield None
        return
    lock_key = f"lock:{key}"
    lock_ttl = LOCK_EXPIRE_SECONDS if expire_seconds is None else expire_seconds
    with Lock(cache, lock_key, expire=lock_ttl):
        yield None


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
