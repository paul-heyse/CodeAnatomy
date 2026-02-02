"""DiskCache-backed index cache for CQ scanning."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from tools.cq.cache.diskcache_profile import (
    DiskCacheProfile,
    cache_for_kind,
    default_cq_diskcache_profile,
    diskcache_stats_snapshot,
)
import msgspec

from tools.cq.cache.tag_index import TagIndex
from tools.cq.core.serialization import dumps_msgpack, loads_msgpack
from tools.cq.index.file_hash import compute_file_hash, compute_file_mtime

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Cache configuration constants
SCHEMA_VERSION = "4"


@dataclass
class CacheStats:
    """Statistics about the index cache."""

    total_files: int
    total_records: int
    rule_version: str
    database_size_bytes: int


class IndexCache:
    """DiskCache-backed cache for incremental code scanning."""

    def __init__(
        self,
        repo_root: Path,
        rule_version: str,
        profile: DiskCacheProfile | None = None,
        *,
        kind: str = "cq_index",
    ) -> None:
        self.repo_root = repo_root
        self.rule_version = rule_version
        self._profile = profile or default_cq_diskcache_profile()
        self._kind = cast(Literal["cq_index"], kind)
        self._cache = cache_for_kind(self._profile, self._kind)
        self._tag_index = TagIndex(self._cache)

    def initialize(self) -> None:
        """No-op initializer for API parity."""

    def needs_rescan(self, file_path: Path, record_types: set[str] | None = None) -> bool:
        if not file_path.exists():
            return False
        file_path_str = self._rel_path(file_path)
        current_hash = compute_file_hash(file_path)
        current_mtime = compute_file_mtime(file_path)
        if current_hash is None or current_mtime is None:
            return True
        entry = self._cache.get(file_path_str, default=None)
        if not isinstance(entry, dict):
            return True
        if entry.get("content_hash") != current_hash:
            return True
        if entry.get("mtime") != current_mtime:
            return True
        if entry.get("rule_version") != self.rule_version:
            return True
        if entry.get("schema_version") != SCHEMA_VERSION:
            return True
        cached_record_types = set(entry.get("record_types", []) or [])
        if record_types is not None and not record_types.issubset(cached_record_types):
            return True
        return False

    def store(
        self,
        file_path: Path,
        records: Sequence[dict[str, object]],
        record_types: set[str],
    ) -> None:
        if not file_path.exists():
            return
        file_path_str = self._rel_path(file_path)
        content_hash = compute_file_hash(file_path)
        mtime = compute_file_mtime(file_path)
        if content_hash is None or mtime is None:
            return
        entry = {
            "file_path": file_path_str,
            "content_hash": content_hash,
            "mtime": mtime,
            "rule_version": self.rule_version,
            "records_payload": dumps_msgpack(records),
            "records_format": "msgpack",
            "schema_version": SCHEMA_VERSION,
            "record_types": sorted(record_types),
        }
        tags = self._entry_tags(file_path_str, record_types)
        self._set_entry(file_path_str, entry, tags)

    def store_many(
        self,
        records_by_file: Mapping[Path, Sequence[dict[str, object]]],
        record_types: set[str],
    ) -> int:
        """Store scan results for many files using a single transaction when available."""
        if not records_by_file:
            return 0
        entries: list[tuple[str, dict[str, object], list[str]]] = []
        for file_path, records in records_by_file.items():
            if not file_path.exists():
                continue
            file_path_str = self._rel_path(file_path)
            content_hash = compute_file_hash(file_path)
            mtime = compute_file_mtime(file_path)
            if content_hash is None or mtime is None:
                continue
            entry = {
                "file_path": file_path_str,
                "content_hash": content_hash,
                "mtime": mtime,
                "rule_version": self.rule_version,
                "records_payload": dumps_msgpack(records),
                "records_format": "msgpack",
                "schema_version": SCHEMA_VERSION,
                "record_types": sorted(record_types),
            }
            tags = self._entry_tags(file_path_str, record_types)
            entries.append((file_path_str, entry, tags))

        if not entries:
            return 0

        transact = getattr(self._cache, "transact", None)
        if callable(transact):
            with self._cache.transact():
                for key, entry, tags in entries:
                    self._set_entry(key, entry, tags, use_transaction=False)
        else:
            for key, entry, tags in entries:
                self._set_entry(key, entry, tags)
        return len(entries)

    def retrieve(
        self,
        file_path: Path,
        record_types: set[str] | None = None,
    ) -> list[dict[str, object]] | None:
        if self.needs_rescan(file_path, record_types):
            return None
        file_path_str = self._rel_path(file_path)
        entry = self._cache.get(file_path_str, default=None)
        if not isinstance(entry, dict):
            return None
        payload = entry.get("records_payload")
        if isinstance(payload, (bytes, bytearray, memoryview)):
            decoded = loads_msgpack(payload)
            return decoded if isinstance(decoded, list) else None
        records_json = entry.get("records_json")
        if isinstance(records_json, str):
            try:
                decoded = msgspec.json.decode(records_json.encode("utf-8"), type=list)
            except Exception:
                decoded = None
            if isinstance(decoded, list):
                return decoded
        return None

    def get_stats(self) -> CacheStats:
        total_files = 0
        total_records = 0
        for key in _iter_cache_keys(self._cache):
            if isinstance(key, str) and self._tag_index.is_tag_index_key(key):
                continue
            entry = self._cache.get(key, default=None)
            if not isinstance(entry, dict):
                continue
            total_files += 1
            payload = entry.get("records_payload")
            if isinstance(payload, (bytes, bytearray, memoryview)):
                decoded = loads_msgpack(payload)
                if isinstance(decoded, list):
                    total_records += len(decoded)
                continue
            records_json = entry.get("records_json")
            if isinstance(records_json, str):
                try:
                    decoded = msgspec.json.decode(records_json.encode("utf-8"), type=list)
                except Exception:
                    decoded = None
                if isinstance(decoded, list):
                    total_records += len(decoded)
        stats = diskcache_stats_snapshot(self._cache)
        volume = stats.get("volume")
        size_bytes = 0
        if isinstance(volume, dict):
            size_bytes = int(volume.get("size") or 0)
        return CacheStats(
            total_files=total_files,
            total_records=total_records,
            rule_version=self.rule_version,
            database_size_bytes=size_bytes,
        )

    def clear(self) -> None:
        self._cache.clear()

    def close(self) -> None:
        self._cache.close()

    def __enter__(self) -> IndexCache:
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _rel_path(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.repo_root.resolve()).as_posix()
        except ValueError:
            return path.resolve().as_posix()

    def _entry_tags(self, file_path_str: str, record_types: set[str]) -> list[str]:
        tags = [f"file:{file_path_str}", f"rule:{self.rule_version}"]
        tags.extend([f"record_type:{rt}" for rt in sorted(record_types)])
        return tags

    def _set_entry(
        self,
        key: str,
        entry: dict[str, object],
        tags: list[str],
        *,
        use_transaction: bool = True,
    ) -> None:
        existing = self._cache.get(key, default=None)
        if isinstance(existing, dict):
            existing_tags = existing.get("tags")
            if isinstance(existing_tags, list):
                self._tag_index.remove(key, existing_tags, use_transaction=use_transaction)
        entry["tags"] = tags
        self._cache.set(key, entry, tag=f"file:{entry.get('file_path', key)}", retry=True)
        self._tag_index.add(key, tags, use_transaction=use_transaction)


def _iter_cache_keys(cache_obj: object):  # type: ignore[no-untyped-def]
    iterkeys = getattr(type(cache_obj), "iterkeys", None)
    if callable(iterkeys):
        return cache_obj.iterkeys()  # type: ignore[no-any-return]
    return iter(cache_obj)


__all__ = [
    "CacheStats",
    "IndexCache",
]
