"""DiskCache-backed query cache for CQ."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import msgspec

from tools.cq.cache.diskcache_profile import (
    DiskCacheProfile,
    cache_for_kind,
    default_cq_diskcache_profile,
    diskcache_stats_snapshot,
)
from tools.cq.cache.file_fingerprint import FileFingerprintCache
from tools.cq.cache.tag_index import TagIndex
from tools.cq.core.schema import CqResult
from tools.cq.core.serialization import dumps_msgpack, loads_msgpack, loads_msgpack_result

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass
class CacheEntry:
    """A cached computation result."""

    key: str
    value: Any
    file_hash: str
    timestamp: float
    file_paths: list[str]


@dataclass
class CacheStats:
    """Statistics about the query cache."""

    total_entries: int
    unique_files: int
    database_size_bytes: int
    oldest_entry: float | None
    newest_entry: float | None
    hits: int | None = None
    misses: int | None = None
    volume: object | None = None


class QueryCache:
    """DiskCache-backed query cache with file hash invalidation."""

    def __init__(
        self,
        repo_root: Path,
        profile: DiskCacheProfile | None = None,
        *,
        kind: str = "cq_query",
    ) -> None:
        self._repo_root = repo_root
        self._profile = profile or default_cq_diskcache_profile()
        self._kind = cast(Literal["cq_query"], kind)
        self._cache = cache_for_kind(self._profile, self._kind)
        self._tag_index = TagIndex(self._cache)
        self._fingerprints = FileFingerprintCache(cache_for_kind(self._profile, "cq_fingerprint"))

    @property
    def repo_root(self) -> Path:
        return self._repo_root

    def get(self, key: str, file_paths: Sequence[Path]) -> Any | None:
        current_hash = _compute_paths_hash(file_paths, self._fingerprints)
        entry = self._cache.get(key, default=None)
        if not isinstance(entry, dict):
            return None
        cached_hash = entry.get("file_hash")
        if cached_hash != current_hash:
            self._drop_entry(key, entry)
            return None
        payload = entry.get("value")
        value_type = entry.get("value_type")
        if isinstance(payload, (bytes, bytearray, memoryview)):
            if value_type == "cq_result":
                return loads_msgpack_result(payload)
            return loads_msgpack(payload)
        if isinstance(payload, dict) and value_type == "cq_result":
            return msgspec.convert(payload, type=CqResult)
        return payload

    def set(self, key: str, value: Any, file_paths: Sequence[Path]) -> None:
        file_hash = _compute_paths_hash(file_paths, self._fingerprints)
        file_path_strs = [self._rel_path(path) for path in file_paths]
        value_type = "generic"
        if isinstance(value, CqResult):
            value_type = "cq_result"
        encoded_value = dumps_msgpack(value)
        entry = {
            "key": key,
            "value": encoded_value,
            "value_type": value_type,
            "file_hash": file_hash,
            "timestamp": time.time(),
            "file_paths": file_path_strs,
        }
        tags = [f"query:{key}"]
        tags.extend([f"file:{path}" for path in file_path_strs])
        if isinstance(value, CqResult):
            schema_version = value.run.schema_version
            if schema_version:
                tags.append(f"schema:{schema_version}")
            sgpy_version = value.run.toolchain.get("sgpy") or "unknown"
            if isinstance(sgpy_version, str):
                tags.append(f"rule:{sgpy_version}")
        elif isinstance(value, dict):
            run = value.get("run")
            if isinstance(run, dict):
                schema_version = run.get("schema_version")
                if isinstance(schema_version, str) and schema_version:
                    tags.append(f"schema:{schema_version}")
                toolchain = run.get("toolchain")
                if isinstance(toolchain, dict):
                    sgpy_version = toolchain.get("sgpy") or "unknown"
                    if isinstance(sgpy_version, str):
                        tags.append(f"rule:{sgpy_version}")
        entry["tags"] = tags
        expire = self._profile.ttl_for("cq_query")
        existing = self._cache.get(key, default=None)
        if isinstance(existing, dict):
            existing_tags = existing.get("tags")
            if isinstance(existing_tags, list):
                self._tag_index.remove(key, existing_tags)
        self._cache.set(key, entry, tag=f"query:{key}", expire=expire, retry=True)
        self._tag_index.add(key, tags)

    def invalidate_file(self, file_path: Path) -> int:
        tag = f"file:{self._rel_path(file_path)}"
        removed = 0
        for key in self._tag_index.keys_for(tag, prune=True):
            if self._drop_entry(key):
                removed += 1
        return removed

    def invalidate_by_hash(self, file_hash: str) -> int:
        removed = 0
        for key in list(_iter_cache_keys(self._cache)):
            if isinstance(key, str) and self._tag_index.is_tag_index_key(key):
                continue
            entry = self._cache.get(key, default=None)
            if not isinstance(entry, dict):
                continue
            if entry.get("file_hash") == file_hash:
                if self._drop_entry(key, entry):
                    removed += 1
        return removed

    def clear(self) -> None:
        self._cache.clear()

    def stats(self) -> CacheStats:
        unique_files: set[str] = set()
        timestamps: list[float] = []
        total_entries = 0
        for key in _iter_cache_keys(self._cache):
            if isinstance(key, str) and self._tag_index.is_tag_index_key(key):
                continue
            entry = self._cache.get(key, default=None)
            if not isinstance(entry, dict):
                continue
            total_entries += 1
            for path in entry.get("file_paths", []) or []:
                if isinstance(path, str):
                    unique_files.add(path)
            ts = entry.get("timestamp")
            if isinstance(ts, (int, float)):
                timestamps.append(float(ts))
        oldest = min(timestamps) if timestamps else None
        newest = max(timestamps) if timestamps else None
        stats = diskcache_stats_snapshot(self._cache)
        volume = stats.get("volume")
        size_bytes = 0
        if isinstance(volume, dict):
            size_bytes = int(volume.get("size") or 0)
        return CacheStats(
            total_entries=total_entries,
            unique_files=len(unique_files),
            database_size_bytes=size_bytes,
            oldest_entry=oldest,
            newest_entry=newest,
            hits=cast("int | None", stats.get("hits")),
            misses=cast("int | None", stats.get("misses")),
            volume=volume,
        )

    def close(self) -> None:
        self._cache.close()

    def __enter__(self) -> QueryCache:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def _rel_path(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self._repo_root.resolve()).as_posix()
        except ValueError:
            return path.resolve().as_posix()

    def _drop_entry(self, key: str, entry: dict[str, object] | None = None) -> bool:
        if entry is None:
            entry = self._cache.get(key, default=None)
        if isinstance(entry, dict):
            tags = entry.get("tags")
            if isinstance(tags, list):
                self._tag_index.remove(key, tags)
        return bool(self._cache.delete(key, retry=True))


def _compute_file_hash(file_path: Path) -> str:
    if not file_path.exists():
        return "missing"
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()[:16]


def _compute_paths_hash(
    file_paths: Sequence[Path],
    fingerprints: FileFingerprintCache | None = None,
) -> str:
    normalized = sorted({path.resolve() for path in file_paths})
    hasher = hashlib.sha256()
    for path in normalized:
        hasher.update(str(path).encode("utf-8"))
        if fingerprints is not None:
            digest = fingerprints.get_or_compute(path)
        else:
            digest = _compute_file_hash(path)
        hasher.update(digest.encode("utf-8"))
    return hasher.hexdigest()[:16]


def _iter_cache_keys(cache_obj: object):  # type: ignore[no-untyped-def]
    iterkeys = getattr(type(cache_obj), "iterkeys", None)
    if callable(iterkeys):
        return cache_obj.iterkeys()  # type: ignore[no-any-return]
    return iter(cache_obj)


def make_cache_key(query_type: str, file: str, params: Mapping[str, object]) -> str:
    params_str = json.dumps(dict(params), sort_keys=True)
    key_input = f"{query_type}:{file}:{params_str}"
    return hashlib.sha256(key_input.encode()).hexdigest()[:32]


__all__ = [
    "CacheEntry",
    "CacheStats",
    "QueryCache",
    "make_cache_key",
]
