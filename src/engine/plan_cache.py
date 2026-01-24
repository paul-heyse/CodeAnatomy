"""Substrait plan cache helpers for DataFusion replay."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cache.diskcache_factory import (
    DiskCacheProfile,
    bulk_cache_set,
    cache_for_kind,
    default_diskcache_profile,
    evict_cache_tag,
)


@dataclass(frozen=True)
class PlanCacheKey:
    """Cache key for Substrait plan bytes."""

    plan_hash: str
    profile_hash: str
    substrait_hash: str

    def as_key(self) -> str:
        """Return a stable cache key string.

        Returns
        -------
        str
            Stable cache key.
        """
        return f"plan:{self.plan_hash}:{self.profile_hash}:{self.substrait_hash}"


@dataclass(frozen=True)
class PlanCacheEntry:
    """Cached Substrait plan bytes keyed by plan/profile hashes."""

    plan_hash: str
    profile_hash: str
    substrait_hash: str
    plan_bytes: bytes
    compilation_lane: str | None = None

    def key(self) -> PlanCacheKey:
        """Return the cache key for this entry.

        Returns
        -------
        PlanCacheKey
            Cache key derived from the entry hashes.
        """
        return PlanCacheKey(
            plan_hash=self.plan_hash,
            profile_hash=self.profile_hash,
            substrait_hash=self.substrait_hash,
        )


@dataclass
class PlanCache:
    """DiskCache-backed cache of Substrait plan bytes."""

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

    def get(self, key: PlanCacheKey) -> bytes | None:
        """Return cached plan bytes for the key when present.

        Returns
        -------
        bytes | None
            Cached plan bytes, or None when missing.
        """
        cache = self._ensure_cache()
        if cache is None:
            return None
        value = cache.get(key.as_key(), default=None, retry=True)
        if value is None:
            return None
        if isinstance(value, PlanCacheEntry):
            return value.plan_bytes
        if isinstance(value, bytes):
            return value
        return None

    def put(self, entry: PlanCacheEntry) -> None:
        """Store a Substrait plan entry in the cache."""
        cache = self._ensure_cache()
        if cache is None:
            return
        cache.set(entry.key().as_key(), entry, tag=entry.profile_hash, retry=True)

    def put_many(self, entries: list[PlanCacheEntry]) -> int:
        """Store multiple Substrait plan entries in the cache.

        Returns
        -------
        int
            Count of entries written.
        """
        if not entries:
            return 0
        cache = self._ensure_cache()
        if cache is None:
            return 0
        payload = {entry.key().as_key(): entry for entry in entries}
        return bulk_cache_set(cache, payload)

    def contains(self, key: PlanCacheKey) -> bool:
        """Return whether the cache contains an entry for the key.

        Returns
        -------
        bool
            ``True`` when the cache has the key.
        """
        cache = self._ensure_cache()
        if cache is None:
            return False
        sentinel = object()
        value = cache.get(key.as_key(), default=sentinel, retry=True)
        return value is not sentinel

    def snapshot(self) -> list[PlanCacheEntry]:
        """Return a list of cached plan entries.

        Returns
        -------
        list[PlanCacheEntry]
            Snapshot of cached entries.
        """
        cache = self._ensure_cache()
        if cache is None:
            return []
        entries: list[PlanCacheEntry] = []
        for key in cache.iterkeys():
            value = cache.get(key, default=None, retry=True)
            if isinstance(value, PlanCacheEntry):
                entries.append(value)
        return entries

    def evict_profile(self, profile_hash: str) -> int:
        """Evict cached plans for a profile hash.

        Returns
        -------
        int
            Count of evicted cache entries.
        """
        profile = self.cache_profile
        if profile is None:
            return 0
        return evict_cache_tag(profile, kind="plan", tag=profile_hash)


if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


__all__ = ["PlanCache", "PlanCacheEntry", "PlanCacheKey"]
