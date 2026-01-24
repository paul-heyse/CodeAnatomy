"""DiskCache configuration and factory helpers."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Literal, cast

from diskcache import Cache, Deque, FanoutCache, Index

from serde_msgspec import dumps_msgpack, to_builtins

type DiskCacheKind = Literal[
    "plan",
    "extract",
    "schema",
    "repo_scan",
    "runtime",
    "queue",
    "index",
]


def _default_cache_root() -> Path:
    env_value = os.environ.get("CODEANATOMY_DISKCACHE_DIR", "").strip()
    if env_value:
        return Path(env_value).expanduser()
    return Path.home() / ".cache" / "codeanatomy" / "diskcache"


def _hash_payload(payload: object) -> str:
    raw = dumps_msgpack(to_builtins(payload))
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class DiskCacheSettings:
    """Settings shared by DiskCache instances."""

    size_limit_bytes: int
    cull_limit: int = 10
    eviction_policy: str = "least-recently-used"
    statistics: bool = False
    tag_index: bool = True
    shards: int | None = None
    timeout_seconds: float = 60.0
    disk_min_file_size: int | None = None

    def fingerprint(self) -> str:
        """Return a stable fingerprint for cache settings.

        Returns
        -------
        str
            Stable fingerprint for settings.
        """
        return _hash_payload(
            {
                "size_limit_bytes": self.size_limit_bytes,
                "cull_limit": self.cull_limit,
                "eviction_policy": self.eviction_policy,
                "statistics": self.statistics,
                "tag_index": self.tag_index,
                "shards": self.shards,
                "timeout_seconds": self.timeout_seconds,
                "disk_min_file_size": self.disk_min_file_size,
            }
        )

@dataclass(frozen=True)
class DiskCacheProfile:
    """DiskCache profile with per-kind overrides and TTL defaults."""

    root: Path = field(default_factory=_default_cache_root)
    base_settings: DiskCacheSettings = field(
        default_factory=lambda: DiskCacheSettings(size_limit_bytes=2 * 1024 * 1024 * 1024)
    )
    overrides: Mapping[DiskCacheKind, DiskCacheSettings] = field(default_factory=dict)
    ttl_seconds: Mapping[DiskCacheKind, float | None] = field(default_factory=dict)

    def settings_for(self, kind: DiskCacheKind) -> DiskCacheSettings:
        """Return settings for a cache kind.

        Returns
        -------
        DiskCacheSettings
            Settings for the cache kind.
        """
        override = self.overrides.get(kind)
        return override if override is not None else self.base_settings

    def ttl_for(self, kind: DiskCacheKind) -> float | None:
        """Return the TTL in seconds for the cache kind when configured.

        Returns
        -------
        float | None
            TTL in seconds when set.
        """
        return self.ttl_seconds.get(kind)

    def fingerprint(self, kind: DiskCacheKind) -> str:
        """Return a fingerprint for the profile+kind combination.

        Returns
        -------
        str
            Stable profile fingerprint for the cache kind.
        """
        payload = {
            "root": str(self.root),
            "kind": kind,
            "settings": self.settings_for(kind).fingerprint(),
            "ttl_seconds": self.ttl_for(kind),
        }
        return _hash_payload(payload)


@cache
def default_diskcache_profile() -> DiskCacheProfile:
    """Return the default DiskCache profile.

    Returns
    -------
    DiskCacheProfile
        Default DiskCache profile.
    """
    base = DiskCacheSettings(size_limit_bytes=2 * 1024 * 1024 * 1024)
    overrides: dict[DiskCacheKind, DiskCacheSettings] = {
        "plan": DiskCacheSettings(size_limit_bytes=512 * 1024 * 1024),
        "extract": DiskCacheSettings(
            size_limit_bytes=8 * 1024 * 1024 * 1024,
            shards=8,
        ),
        "schema": DiskCacheSettings(size_limit_bytes=256 * 1024 * 1024),
        "repo_scan": DiskCacheSettings(size_limit_bytes=512 * 1024 * 1024),
        "runtime": DiskCacheSettings(size_limit_bytes=256 * 1024 * 1024),
    }
    ttl_seconds: dict[DiskCacheKind, float | None] = {
        "plan": None,
        "extract": 24 * 60 * 60,
        "schema": 5 * 60,
        "repo_scan": 30 * 60,
        "runtime": 24 * 60 * 60,
    }
    return DiskCacheProfile(
        root=_default_cache_root(),
        base_settings=base,
        overrides=overrides,
        ttl_seconds=ttl_seconds,
    )


_CACHE_POOL: dict[str, Cache | FanoutCache] = {}


def cache_for_kind(profile: DiskCacheProfile, kind: DiskCacheKind) -> Cache | FanoutCache:
    """Return a cached Cache/FanoutCache instance for the kind.

    Returns
    -------
    Cache | FanoutCache
        Cache instance for the kind.
    """
    fingerprint = profile.fingerprint(kind)
    cache = _CACHE_POOL.get(fingerprint)
    if cache is not None:
        return cache
    settings = profile.settings_for(kind)
    base_dir = profile.root / kind
    if settings.shards is not None and settings.shards > 1:
        cache = FanoutCache(
            str(base_dir),
            shards=settings.shards,
            size_limit=settings.size_limit_bytes,
            cull_limit=settings.cull_limit,
            eviction_policy=settings.eviction_policy,
            statistics=settings.statistics,
            tag_index=settings.tag_index,
            timeout=int(settings.timeout_seconds),
            disk_min_file_size=settings.disk_min_file_size,
        )
    else:
        cache = Cache(
            str(base_dir),
            size_limit=settings.size_limit_bytes,
            cull_limit=settings.cull_limit,
            eviction_policy=settings.eviction_policy,
            statistics=settings.statistics,
            tag_index=settings.tag_index,
            timeout=int(settings.timeout_seconds),
            disk_min_file_size=settings.disk_min_file_size,
        )
    _CACHE_POOL[fingerprint] = cache
    return cache


def _stats_mapping(stats: object) -> dict[str, object]:
    if isinstance(stats, Mapping):
        return dict(stats)
    to_dict = getattr(stats, "_asdict", None)
    if callable(to_dict):
        return cast("dict[str, object]", to_dict())
    if isinstance(stats, tuple):
        keys = (
            "hits",
            "misses",
            "count",
            "size",
            "size_limit",
            "cull_limit",
            "eviction_policy",
        )
        padded = list(stats) + [None] * max(0, len(keys) - len(stats))
        return dict(zip(keys, padded, strict=False))
    return {}


def diskcache_stats_snapshot(cache: Cache | FanoutCache) -> dict[str, object]:
    """Return a stats snapshot for the provided DiskCache instance.

    Returns
    -------
    dict[str, object]
        Snapshot of DiskCache stats and volume.
    """
    stats = _stats_mapping(cache.stats())
    return {
        "volume": cache.volume(),
        "hits": stats.get("hits"),
        "misses": stats.get("misses"),
        "count": stats.get("count"),
        "size": stats.get("size"),
        "settings": {
            "size_limit": stats.get("size_limit"),
            "eviction_policy": stats.get("eviction_policy"),
            "cull_limit": stats.get("cull_limit"),
        },
    }


def build_deque(
    profile: DiskCacheProfile,
    *,
    name: str,
    maxlen: int | None = None,
) -> Deque:
    """Return a persistent DiskCache Deque.

    Returns
    -------
    Deque
        Persistent deque backed by DiskCache.
    """
    cache_dir = profile.root / "queue" / name
    return Deque(str(cache_dir), maxlen=maxlen)


def build_index(profile: DiskCacheProfile, *, name: str) -> Index:
    """Return a persistent DiskCache Index.

    Returns
    -------
    Index
        Persistent index backed by DiskCache.
    """
    settings = profile.settings_for("index")
    cache_dir = profile.root / "index" / name
    return Index(
        str(cache_dir),
        size_limit=settings.size_limit_bytes,
        cull_limit=settings.cull_limit,
        eviction_policy="none",
        statistics=settings.statistics,
        tag_index=settings.tag_index,
        timeout=settings.timeout_seconds,
        disk_min_file_size=settings.disk_min_file_size,
    )


__all__ = [
    "DiskCacheKind",
    "DiskCacheProfile",
    "DiskCacheSettings",
    "build_deque",
    "build_index",
    "cache_for_kind",
    "default_diskcache_profile",
    "diskcache_stats_snapshot",
]
