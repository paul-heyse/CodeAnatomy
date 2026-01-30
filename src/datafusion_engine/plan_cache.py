"""Plan cache helpers for DataFusion plan proto reuse."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cache.diskcache_factory import (
    DiskCacheProfile,
    cache_for_kind,
    default_diskcache_profile,
)
from utils.hashing import CacheKeyBuilder

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


@dataclass(frozen=True)
class PlanCacheEntry:
    """Cached plan proto payloads keyed by plan identity hash."""

    plan_identity_hash: str
    plan_fingerprint: str | None = None
    substrait_bytes: bytes | None = None
    logical_plan_proto: bytes | None = None
    optimized_plan_proto: bytes | None = None
    execution_plan_proto: bytes | None = None

    def cache_key(self) -> str:
        """Return the cache key for this entry.

        Returns
        -------
        str
            Cache key for the plan entry.
        """
        return _plan_cache_key(self.plan_identity_hash)


@dataclass
class PlanProtoCache:
    """Disk-backed cache for DataFusion plan proto payloads."""

    cache_profile: DiskCacheProfile | None = field(default_factory=default_diskcache_profile)
    _cache: Cache | FanoutCache | None = field(default=None, init=False, repr=False)

    def _ensure_cache(self) -> Cache | FanoutCache | None:
        if self._cache is not None:
            return self._cache
        profile = self.cache_profile
        if profile is None:
            return None
        cache = cache_for_kind(profile, "plan")
        self._cache = cache
        return cache

    def get(self, plan_identity_hash: str) -> PlanCacheEntry | None:
        """Return cached plan entry for a plan identity hash.

        Parameters
        ----------
        plan_identity_hash
            Stable identity hash for the requested plan.

        Returns
        -------
        PlanCacheEntry | None
            Cached plan entry when available.
        """
        cache = self._ensure_cache()
        if cache is None:
            return None
        key = _plan_cache_key(plan_identity_hash)
        value = cache.get(key, default=None, retry=True)
        if isinstance(value, PlanCacheEntry):
            return value
        return None

    def put(self, entry: PlanCacheEntry) -> None:
        """Store a plan cache entry."""
        cache = self._ensure_cache()
        if cache is None:
            return
        cache.set(entry.cache_key(), entry, tag=entry.plan_identity_hash, retry=True)

    def contains(self, plan_identity_hash: str) -> bool:
        """Return whether a plan identity hash is cached.

        Parameters
        ----------
        plan_identity_hash
            Stable identity hash for the requested plan.

        Returns
        -------
        bool
            True when the plan is present in the cache.
        """
        cache = self._ensure_cache()
        if cache is None:
            return False
        sentinel = object()
        value = cache.get(_plan_cache_key(plan_identity_hash), default=sentinel, retry=True)
        return value is not sentinel

    def snapshot(self) -> list[PlanCacheEntry]:
        """Return a snapshot of cached plan entries.

        Returns
        -------
        list[PlanCacheEntry]
            Cached entries captured from the backing store.
        """
        cache = self._ensure_cache()
        if cache is None:
            return []
        entries: list[PlanCacheEntry] = []
        for key in cache.iterkeys():
            if not isinstance(key, str) or not key.startswith("plan_proto:"):
                continue
            value = cache.get(key, default=None, retry=True)
            if isinstance(value, PlanCacheEntry):
                entries.append(value)
        return entries


def _plan_cache_key(plan_identity_hash: str) -> str:
    builder = CacheKeyBuilder(prefix="plan_proto")
    builder.add("plan_identity_hash", plan_identity_hash)
    return builder.build()


__all__ = ["PlanCacheEntry", "PlanProtoCache"]
