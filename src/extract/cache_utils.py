"""DiskCache helpers for extract workflows."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING

from cache.diskcache_factory import DiskCacheKind, DiskCacheProfile, cache_for_kind
from serde_msgspec import dumps_msgpack, to_builtins

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from arrowdsl.core.execution_context import ExecutionContext


def _hash_payload(payload: object) -> str:
    raw = dumps_msgpack(to_builtins(payload))
    return hashlib.sha256(raw).hexdigest()


def stable_cache_key(prefix: str, payload: Mapping[str, object]) -> str:
    """Return a stable string key for DiskCache usage.

    Returns
    -------
    str
        Stable cache key string.
    """
    digest = _hash_payload(payload)
    return f"{prefix}:{digest}"


def diskcache_profile_from_ctx(ctx: ExecutionContext | None) -> DiskCacheProfile | None:
    """Return the DiskCache profile for an execution context.

    Returns
    -------
    DiskCacheProfile | None
        DiskCache profile when configured.
    """
    if ctx is None or ctx.runtime.datafusion is None:
        return None
    return ctx.runtime.datafusion.diskcache_profile


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


__all__ = [
    "cache_for_extract",
    "cache_for_kind_optional",
    "cache_ttl_seconds",
    "diskcache_profile_from_ctx",
    "stable_cache_key",
]
