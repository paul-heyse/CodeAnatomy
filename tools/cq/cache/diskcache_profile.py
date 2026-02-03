"""CQ-local DiskCache configuration and helpers.

This module intentionally does not import from src/.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Literal, TypedDict, cast

import os
from contextlib import contextmanager

try:  # pragma: no cover - optional dependency
    from diskcache import Cache, FanoutCache
    _DISKCACHE_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    _DISKCACHE_AVAILABLE = False

    class Cache:
        """Fallback Cache stub when diskcache is unavailable."""

    class FanoutCache:
        """Fallback FanoutCache stub when diskcache is unavailable."""

from tools.cq.cache.tag_index import TagIndex

DiskCacheKind = Literal[
    "cq_query",
    "cq_index",
    "cq_coordination",
    "cq_fingerprint",
]


def _env_value(key: str) -> str | None:
    return os.environ.get(key)


def _env_int(key: str) -> int | None:
    value = _env_value(key)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _env_float(key: str) -> float | None:
    value = _env_value(key)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _default_cache_root() -> Path:
    env_root = _env_value("CQ_DISKCACHE_DIR")
    if env_root:
        return Path(env_root).expanduser()
    return Path.home() / ".cache" / "codeanatomy" / "cq"


def _config_fingerprint(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class DiskCacheSettings:
    """Settings shared by CQ DiskCache instances."""

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
        return _config_fingerprint(self.fingerprint_payload())


class DiskCacheKwargs(TypedDict, total=False):
    cull_limit: int
    eviction_policy: str
    statistics: bool
    tag_index: bool
    disk_min_file_size: int
    sqlite_journal_mode: str
    sqlite_mmap_size: int
    sqlite_synchronous: str


@dataclass(frozen=True)
class DiskCacheProfile:
    """CQ DiskCache profile with per-kind overrides and TTL defaults."""

    root: Path = field(default_factory=_default_cache_root)
    base_settings: DiskCacheSettings = field(
        default_factory=lambda: DiskCacheSettings(size_limit_bytes=512 * 1024 * 1024)
    )
    overrides: Mapping[DiskCacheKind, DiskCacheSettings] = field(default_factory=dict)
    ttl_seconds: Mapping[DiskCacheKind, float | None] = field(default_factory=dict)

    def settings_for(self, kind: DiskCacheKind) -> DiskCacheSettings:
        override = self.overrides.get(kind)
        return override if override is not None else self.base_settings

    def ttl_for(self, kind: DiskCacheKind) -> float | None:
        return self.ttl_seconds.get(kind)

    def fingerprint(self, kind: DiskCacheKind) -> str:
        payload = {
            "root": str(self.root),
            "kind": kind,
            "settings": self.settings_for(kind).fingerprint(),
            "ttl_seconds": self.ttl_for(kind),
        }
        return _config_fingerprint(payload)


@cache
def default_cq_diskcache_profile() -> DiskCacheProfile:
    """Return default CQ diskcache profile with env overrides."""
    base_size = _env_int("CQ_DISKCACHE_BASE_SIZE_LIMIT")
    query_size = _env_int("CQ_DISKCACHE_QUERY_SIZE_LIMIT")
    index_size = _env_int("CQ_DISKCACHE_INDEX_SIZE_LIMIT")
    fingerprint_size = _env_int("CQ_DISKCACHE_FINGERPRINT_SIZE_LIMIT")
    base = DiskCacheSettings(
        size_limit_bytes=base_size or 512 * 1024 * 1024,
        tag_index=True,
    )
    overrides: dict[DiskCacheKind, DiskCacheSettings] = {
        "cq_query": DiskCacheSettings(
            size_limit_bytes=query_size or 512 * 1024 * 1024,
            shards=_env_int("CQ_DISKCACHE_QUERY_SHARDS") or 4,
        ),
        "cq_index": DiskCacheSettings(
            size_limit_bytes=index_size or 256 * 1024 * 1024,
            eviction_policy="none",
        ),
        "cq_coordination": DiskCacheSettings(
            size_limit_bytes=64 * 1024 * 1024,
            eviction_policy="none",
        ),
        "cq_fingerprint": DiskCacheSettings(
            size_limit_bytes=fingerprint_size or 64 * 1024 * 1024,
        ),
    }
    ttl_seconds: dict[DiskCacheKind, float | None] = {
        "cq_query": _env_float("CQ_DISKCACHE_QUERY_TTL_SECONDS"),
        "cq_index": None,
        "cq_coordination": None,
        "cq_fingerprint": None,
    }
    return DiskCacheProfile(
        root=_default_cache_root(),
        base_settings=base,
        overrides=overrides,
        ttl_seconds=ttl_seconds,
    )


_CACHE_POOL: dict[str, Cache | FanoutCache] = {}


class NoOpCache:
    """Fallback cache when diskcache is unavailable."""

    def get(self, key: str, default: object | None = None) -> object | None:
        _ = key
        return default

    def set(self, key: str, value: object, **kwargs) -> bool:
        _ = (key, value, kwargs)
        return False

    def delete(self, key: str, **kwargs) -> bool:
        _ = (key, kwargs)
        return False

    def evict(self, tag: str, **kwargs) -> int:
        _ = (tag, kwargs)
        return 0

    def iterkeys(self) -> Iterator[str]:
        return iter(())

    def clear(self) -> int:
        return 0

    def stats(self) -> dict[str, object]:
        return {}

    def volume(self) -> dict[str, object]:
        return {}

    def expire(self, **kwargs) -> int:
        _ = kwargs
        return 0

    def cull(self, **kwargs) -> int:
        _ = kwargs
        return 0

    def check(self, **kwargs) -> list[object]:
        _ = kwargs
        return []

    def close(self) -> None:
        return None

    @contextmanager
    def transact(self) -> Iterator["NoOpCache"]:
        yield self


@dataclass(frozen=True)
class DiskCacheMaintenance:
    """Maintenance result for a CQ cache kind."""

    kind: DiskCacheKind
    expired: int
    culled: int
    check_errors: int | None = None


class DiskCacheStats(TypedDict, total=False):
    volume: object
    hits: object
    misses: object
    count: object
    size: object
    settings: Mapping[str, object]



def cache_for_kind(profile: DiskCacheProfile, kind: DiskCacheKind) -> Cache | FanoutCache:
    if not _DISKCACHE_AVAILABLE:
        return cast("Cache | FanoutCache", NoOpCache())
    fingerprint = profile.fingerprint(kind)
    cache_obj = _CACHE_POOL.get(fingerprint)
    if cache_obj is not None:
        return cache_obj
    settings = profile.settings_for(kind)
    base_dir = profile.root / kind
    settings_kwargs = _settings_kwargs(settings)
    if settings.shards is not None and settings.shards > 1:
        cache_obj = FanoutCache(
            str(base_dir),
            shards=settings.shards,
            size_limit=settings.size_limit_bytes,
            timeout=int(settings.timeout_seconds),
            **settings_kwargs,
        )
    else:
        cache_obj = Cache(
            str(base_dir),
            size_limit=settings.size_limit_bytes,
            timeout=int(settings.timeout_seconds),
            **settings_kwargs,
        )
    _CACHE_POOL[fingerprint] = cache_obj
    return cache_obj


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


def diskcache_stats_snapshot(cache_obj: Cache | FanoutCache) -> DiskCacheStats:
    stats = _stats_mapping(cache_obj.stats())
    return {
        "volume": cache_obj.volume(),
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


def run_cache_maintenance(
    profile: DiskCacheProfile,
    *,
    kind: DiskCacheKind,
    include_check: bool = False,
) -> DiskCacheMaintenance:
    cache_obj = cache_for_kind(profile, kind)
    expired = int(cache_obj.expire(retry=True))
    culled = int(cache_obj.cull(retry=True))
    check_errors = None
    if include_check:
        errors = cache_obj.check(retry=True)
        check_errors = len(errors) if isinstance(errors, list) else int(bool(errors))
    return DiskCacheMaintenance(
        kind=kind,
        expired=expired,
        culled=culled,
        check_errors=check_errors,
    )


def evict_cache_tag(profile: DiskCacheProfile, *, kind: DiskCacheKind, tag: str) -> int:
    cache_obj = cache_for_kind(profile, kind)
    tag_index = TagIndex(cache_obj)
    keys = tag_index.keys_for(tag, prune=True)
    if not keys:
        return int(cache_obj.evict(tag, retry=True))

    removed = 0
    transact = getattr(cache_obj, "transact", None)
    if callable(transact):
        with cache_obj.transact():
            removed = _evict_keys(cache_obj, tag_index, keys)
    else:
        removed = _evict_keys(cache_obj, tag_index, keys)
    return removed


def _evict_keys(cache_obj: Cache | FanoutCache, tag_index: TagIndex, keys: set[str]) -> int:
    removed = 0
    for key in keys:
        entry = cache_obj.get(key, default=None)
        if isinstance(entry, dict):
            tags = entry.get("tags")
            if isinstance(tags, list):
                tag_index.remove(key, tags, use_transaction=False)
        if cache_obj.delete(key, retry=True):
            removed += 1
    return removed


__all__ = [
    "DiskCacheKind",
    "DiskCacheMaintenance",
    "DiskCacheProfile",
    "DiskCacheSettings",
    "DiskCacheStats",
    "cache_for_kind",
    "default_cq_diskcache_profile",
    "diskcache_stats_snapshot",
    "evict_cache_tag",
    "run_cache_maintenance",
]
