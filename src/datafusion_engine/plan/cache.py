"""Plan cache helpers for DataFusion plan proto and Substrait replay."""

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
from core.fingerprinting import CompositeFingerprint
from serde_msgspec import StructBaseHotPath
from utils.hashing import CacheKeyBuilder

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


# -----------------------------------------------------------------------------
# Plan Proto Cache (identity-hash keyed)
# -----------------------------------------------------------------------------


class PlanProtoCacheEntry(StructBaseHotPath, frozen=True):
    """Cached plan proto payloads keyed by plan identity hash."""

    plan_identity_hash: str
    substrait_bytes: bytes
    plan_fingerprint: str | None = None
    logical_plan_proto: bytes | None = None
    optimized_plan_proto: bytes | None = None
    execution_plan_proto: bytes | None = None

    def cache_key(self) -> str:
        """Return the cache key for this entry.

        Returns:
        -------
        str
            Cache key for the plan entry.
        """
        return _plan_proto_cache_key(self.plan_identity_hash)


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

    def get(self, plan_identity_hash: str) -> PlanProtoCacheEntry | None:
        """Return cached plan entry for a plan identity hash.

        Parameters
        ----------
        plan_identity_hash
            Stable identity hash for the requested plan.

        Returns:
        -------
        PlanProtoCacheEntry | None
            Cached plan entry when available.
        """
        cache = self._ensure_cache()
        if cache is None:
            return None
        key = _plan_proto_cache_key(plan_identity_hash)
        value = cache.get(key, default=None, retry=True)
        if isinstance(value, PlanProtoCacheEntry):
            return value
        return None

    def put(self, entry: PlanProtoCacheEntry) -> None:
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

        Returns:
        -------
        bool
            True when the plan is present in the cache.
        """
        cache = self._ensure_cache()
        if cache is None:
            return False
        sentinel = object()
        value = cache.get(_plan_proto_cache_key(plan_identity_hash), default=sentinel, retry=True)
        return value is not sentinel

    def snapshot(self) -> list[PlanProtoCacheEntry]:
        """Return a snapshot of cached plan entries.

        Returns:
        -------
        list[PlanProtoCacheEntry]
            Cached entries captured from the backing store.
        """
        cache = self._ensure_cache()
        if cache is None:
            return []
        entries: list[PlanProtoCacheEntry] = []
        for key in cache.iterkeys():
            if not isinstance(key, str) or not key.startswith("plan_proto:"):
                continue
            value = cache.get(key, default=None, retry=True)
            if isinstance(value, PlanProtoCacheEntry):
                entries.append(value)
        return entries


def _plan_proto_cache_key(plan_identity_hash: str) -> str:
    builder = CacheKeyBuilder(prefix="plan_proto")
    builder.add("plan_identity_hash", plan_identity_hash)
    return builder.build()


# -----------------------------------------------------------------------------
# Substrait Plan Cache (multi-hash keyed)
# -----------------------------------------------------------------------------


class PlanCacheKey(StructBaseHotPath, frozen=True):
    """Cache key for Substrait plan bytes."""

    profile_hash: str
    substrait_hash: str
    plan_fingerprint: str
    udf_snapshot_hash: str
    function_registry_hash: str
    information_schema_hash: str
    required_udfs_hash: str
    required_rewrite_tags_hash: str
    settings_hash: str
    delta_inputs_hash: str

    def as_key(self) -> str:
        """Return a stable cache key string.

        Returns:
        -------
        str
            Stable cache key.
        """
        return self.composite_fingerprint().as_cache_key(prefix="plan")

    def composite_fingerprint(self) -> CompositeFingerprint:
        """Return a composite fingerprint for the cache key.

        Returns:
        -------
        CompositeFingerprint
            Composite fingerprint for cache-key computation.
        """
        return CompositeFingerprint.from_components(
            1,
            profile_hash=self.profile_hash,
            substrait_hash=self.substrait_hash,
            plan_fingerprint=self.plan_fingerprint,
            udf_snapshot_hash=self.udf_snapshot_hash,
            function_registry_hash=self.function_registry_hash,
            information_schema_hash=self.information_schema_hash,
            required_udfs_hash=self.required_udfs_hash,
            required_rewrite_tags_hash=self.required_rewrite_tags_hash,
            settings_hash=self.settings_hash,
            delta_inputs_hash=self.delta_inputs_hash,
        )


class PlanCacheEntry(StructBaseHotPath, frozen=True):
    """Cached Substrait plan bytes keyed by profile and Substrait hashes."""

    profile_hash: str
    substrait_hash: str
    plan_bytes: bytes
    plan_fingerprint: str = ""
    udf_snapshot_hash: str = ""
    function_registry_hash: str = ""
    information_schema_hash: str = ""
    required_udfs_hash: str = ""
    required_rewrite_tags_hash: str = ""
    settings_hash: str = ""
    delta_inputs_hash: str = ""
    compilation_lane: str | None = None

    def key(self) -> PlanCacheKey:
        """Return the cache key for this entry.

        Returns:
        -------
        PlanCacheKey
            Cache key derived from the entry hashes.
        """
        return PlanCacheKey(
            profile_hash=self.profile_hash,
            substrait_hash=self.substrait_hash,
            plan_fingerprint=self.plan_fingerprint,
            udf_snapshot_hash=self.udf_snapshot_hash,
            function_registry_hash=self.function_registry_hash,
            information_schema_hash=self.information_schema_hash,
            required_udfs_hash=self.required_udfs_hash,
            required_rewrite_tags_hash=self.required_rewrite_tags_hash,
            settings_hash=self.settings_hash,
            delta_inputs_hash=self.delta_inputs_hash,
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

        Returns:
        -------
        bytes | None
            Cached plan bytes, or None when missing.
        """
        cache = self._ensure_cache()
        if cache is None:
            return None
        primary_key = key.as_key()
        value = cache.get(primary_key, default=None, retry=True)
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

        Returns:
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

        Returns:
        -------
        bool
            ``True`` when the cache has the key.
        """
        cache = self._ensure_cache()
        if cache is None:
            return False
        sentinel = object()
        primary_key = key.as_key()
        value = cache.get(primary_key, default=sentinel, retry=True)
        return value is not sentinel

    def snapshot(self) -> list[PlanCacheEntry]:
        """Return a list of cached plan entries.

        Returns:
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

        Returns:
        -------
        int
            Count of evicted cache entries.
        """
        profile = self.cache_profile
        if profile is None:
            return 0
        return evict_cache_tag(profile, kind="plan", tag=profile_hash)


__all__ = [
    "PlanCache",
    "PlanCacheEntry",
    "PlanCacheKey",
    "PlanProtoCache",
    "PlanProtoCacheEntry",
]
