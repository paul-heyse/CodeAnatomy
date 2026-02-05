"""DiskCache configuration and factory helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Literal, TypedDict, cast

import msgspec
from diskcache import Cache, Deque, FanoutCache, Index

from core.config_base import config_fingerprint
from serde_msgspec import StructBaseStrict
from utils.env_utils import env_value

type DiskCacheKind = Literal[
    "plan",
    "extract",
    "schema",
    "repo_scan",
    "runtime",
    "queue",
    "index",
    "coordination",
]


def _default_cache_root() -> Path:
    """Return the default DiskCache root path.

    Returns
    -------
    pathlib.Path
        Default cache root path.
    """
    root = env_value("CODEANATOMY_DISKCACHE_DIR")
    if root:
        return Path(root).expanduser()
    return Path.home() / ".cache" / "codeanatomy" / "diskcache"


class DiskCacheSettings(StructBaseStrict, frozen=True):
    """Settings shared by DiskCache instances."""

    size_limit_bytes: int
    cull_limit: int = 10
    eviction_policy: str = "least-recently-used"
    statistics: bool = True
    tag_index: bool = True
    shards: int | None = None
    timeout_seconds: float = 60.0
    disk_min_file_size: int | None = None
    sqlite_journal_mode: str | None = "wal"
    sqlite_mmap_size: int | None = None
    sqlite_synchronous: str | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return canonical payload for settings fingerprinting.

        Returns
        -------
        Mapping[str, object]
            Payload used for settings fingerprinting.
        """
        return {
            "size_limit_bytes": self.size_limit_bytes,
            "cull_limit": self.cull_limit,
            "eviction_policy": self.eviction_policy,
            "statistics": self.statistics,
            "tag_index": self.tag_index,
            "shards": self.shards,
            "timeout_seconds": self.timeout_seconds,
            "disk_min_file_size": self.disk_min_file_size,
            "sqlite_journal_mode": self.sqlite_journal_mode,
            "sqlite_mmap_size": self.sqlite_mmap_size,
            "sqlite_synchronous": self.sqlite_synchronous,
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for cache settings.

        Returns
        -------
        str
            Stable fingerprint for settings.
        """
        return config_fingerprint(self.fingerprint_payload())


class DiskCacheKwargs(TypedDict, total=False):
    cull_limit: int
    eviction_policy: str
    statistics: bool
    tag_index: bool
    disk_min_file_size: int
    sqlite_journal_mode: str
    sqlite_mmap_size: int
    sqlite_synchronous: str


class DiskCacheProfile(StructBaseStrict, frozen=True):
    """DiskCache profile with per-kind overrides and TTL defaults."""

    root: Path = msgspec.field(default_factory=_default_cache_root)
    base_settings: DiskCacheSettings = msgspec.field(
        default_factory=lambda: DiskCacheSettings(size_limit_bytes=2 * 1024 * 1024 * 1024)
    )
    overrides: Mapping[DiskCacheKind, DiskCacheSettings] = msgspec.field(default_factory=dict)
    ttl_seconds: Mapping[DiskCacheKind, float | None] = msgspec.field(default_factory=dict)

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
        return config_fingerprint(payload)


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
            disk_min_file_size=512 * 1024,
        ),
        "schema": DiskCacheSettings(size_limit_bytes=256 * 1024 * 1024),
        "repo_scan": DiskCacheSettings(size_limit_bytes=512 * 1024 * 1024),
        "runtime": DiskCacheSettings(size_limit_bytes=256 * 1024 * 1024),
        "queue": DiskCacheSettings(
            size_limit_bytes=128 * 1024 * 1024,
            eviction_policy="none",
        ),
        "index": DiskCacheSettings(
            size_limit_bytes=128 * 1024 * 1024,
            eviction_policy="none",
        ),
        "coordination": DiskCacheSettings(
            size_limit_bytes=64 * 1024 * 1024,
            eviction_policy="none",
        ),
    }
    ttl_seconds: dict[DiskCacheKind, float | None] = {
        "plan": None,
        "extract": 24 * 60 * 60,
        "schema": 5 * 60,
        "repo_scan": 30 * 60,
        "runtime": 24 * 60 * 60,
        "queue": None,
        "index": None,
        "coordination": None,
    }
    return DiskCacheProfile(
        root=_default_cache_root(),
        base_settings=base,
        overrides=overrides,
        ttl_seconds=ttl_seconds,
    )


_CACHE_POOL: dict[str, Cache | FanoutCache] = {}


@dataclass(frozen=True)
class DiskCacheMaintenance:
    """Maintenance result for a cache kind."""

    kind: DiskCacheKind
    expired: int
    culled: int
    check_errors: int | None = None


def run_cache_maintenance(
    profile: DiskCacheProfile,
    *,
    kind: DiskCacheKind,
    include_check: bool = False,
) -> DiskCacheMaintenance:
    """Run cache maintenance for a cache kind.

    Returns
    -------
    DiskCacheMaintenance
        Maintenance results for the cache kind.
    """
    cache = cache_for_kind(profile, kind)
    expired = int(cache.expire(retry=True))
    culled = int(cache.cull(retry=True))
    check_errors = None
    if include_check:
        errors = cache.check(retry=True)
        check_errors = len(errors) if isinstance(errors, list) else int(bool(errors))
    return DiskCacheMaintenance(
        kind=kind,
        expired=expired,
        culled=culled,
        check_errors=check_errors,
    )


def run_profile_maintenance(
    profile: DiskCacheProfile,
    *,
    kinds: tuple[DiskCacheKind, ...] | None = None,
    include_check: bool = False,
) -> list[DiskCacheMaintenance]:
    """Run maintenance across a set of cache kinds.

    Returns
    -------
    list[DiskCacheMaintenance]
        Maintenance results for each cache kind.
    """
    targets = kinds or ("plan", "extract", "schema", "repo_scan", "runtime")
    return [
        run_cache_maintenance(profile, kind=kind, include_check=include_check) for kind in targets
    ]


def evict_cache_tag(
    profile: DiskCacheProfile,
    *,
    kind: DiskCacheKind,
    tag: str,
) -> int:
    """Evict cache entries for a tag and kind.

    Returns
    -------
    int
        Count of evicted entries.
    """
    cache = cache_for_kind(profile, kind)
    return int(cache.evict(tag, retry=True))


def bulk_cache_set(
    cache: Cache | FanoutCache | None,
    entries: Mapping[str, object],
    *,
    expire: float | None = None,
    tag: str | None = None,
    read: bool = False,
) -> int:
    """Set multiple cache entries, using transactions when available.

    Returns
    -------
    int
        Count of entries written.
    """
    if cache is None or not entries:
        return 0
    if isinstance(cache, FanoutCache):
        for key, value in entries.items():
            cache.set(
                key,
                value,
                expire=expire,
                tag=tag,
                read=read,
                retry=True,
            )
        return len(entries)
    with cache.transact():
        for key, value in entries.items():
            cache.set(
                key,
                value,
                expire=expire,
                tag=tag,
                read=read,
                retry=True,
            )
    return len(entries)


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
    settings_kwargs = _settings_kwargs(settings)
    if settings.shards is not None and settings.shards > 1:
        cache = FanoutCache(
            str(base_dir),
            shards=settings.shards,
            size_limit=settings.size_limit_bytes,
            timeout=int(settings.timeout_seconds),
            **settings_kwargs,
        )
    else:
        cache = Cache(
            str(base_dir),
            size_limit=settings.size_limit_bytes,
            timeout=int(settings.timeout_seconds),
            **settings_kwargs,
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


def _settings_kwargs(settings: DiskCacheSettings) -> DiskCacheKwargs:
    kwargs: DiskCacheKwargs = {
        "cull_limit": settings.cull_limit,
        "eviction_policy": settings.eviction_policy,
        "statistics": settings.statistics,
        "tag_index": settings.tag_index,
    }
    if settings.disk_min_file_size is not None:
        kwargs["disk_min_file_size"] = settings.disk_min_file_size
    if settings.sqlite_journal_mode is not None:
        kwargs["sqlite_journal_mode"] = settings.sqlite_journal_mode
    if settings.sqlite_mmap_size is not None:
        kwargs["sqlite_mmap_size"] = settings.sqlite_mmap_size
    if settings.sqlite_synchronous is not None:
        kwargs["sqlite_synchronous"] = settings.sqlite_synchronous
    return kwargs


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
        sqlite_journal_mode=settings.sqlite_journal_mode,
        sqlite_mmap_size=settings.sqlite_mmap_size,
        sqlite_synchronous=settings.sqlite_synchronous,
    )


__all__ = [
    "DiskCacheKind",
    "DiskCacheMaintenance",
    "DiskCacheProfile",
    "DiskCacheSettings",
    "build_deque",
    "build_index",
    "bulk_cache_set",
    "cache_for_kind",
    "default_diskcache_profile",
    "diskcache_stats_snapshot",
    "evict_cache_tag",
    "run_cache_maintenance",
    "run_profile_maintenance",
]
