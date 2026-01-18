"""Substrait plan cache helpers for DataFusion replay."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PlanCacheKey:
    """Cache key for Substrait plan bytes."""

    plan_hash: str
    profile_hash: str


@dataclass(frozen=True)
class PlanCacheEntry:
    """Cached Substrait plan bytes keyed by plan/profile hashes."""

    plan_hash: str
    profile_hash: str
    plan_bytes: bytes

    def key(self) -> PlanCacheKey:
        """Return the cache key for this entry.

        Returns
        -------
        PlanCacheKey
            Cache key derived from the entry hashes.
        """
        return PlanCacheKey(plan_hash=self.plan_hash, profile_hash=self.profile_hash)


@dataclass
class PlanCache:
    """In-memory cache of Substrait plan bytes."""

    entries: dict[PlanCacheKey, PlanCacheEntry] = field(default_factory=dict)

    def get(self, key: PlanCacheKey) -> bytes | None:
        """Return cached plan bytes for the key when present.

        Returns
        -------
        bytes | None
            Cached plan bytes, or None when missing.
        """
        entry = self.entries.get(key)
        return entry.plan_bytes if entry is not None else None

    def put(self, entry: PlanCacheEntry) -> None:
        """Store a Substrait plan entry in the cache."""
        self.entries[entry.key()] = entry

    def contains(self, key: PlanCacheKey) -> bool:
        """Return whether the cache contains an entry for the key.

        Returns
        -------
        bool
            ``True`` when the cache has the key.
        """
        return key in self.entries

    def snapshot(self) -> list[PlanCacheEntry]:
        """Return a list of cached plan entries.

        Returns
        -------
        list[PlanCacheEntry]
            Snapshot of cached entries.
        """
        return list(self.entries.values())


__all__ = ["PlanCache", "PlanCacheEntry", "PlanCacheKey"]
