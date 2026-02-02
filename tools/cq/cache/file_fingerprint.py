"""File fingerprint cache for CQ query caching."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


@dataclass(frozen=True)
class FileFingerprint:
    """Lightweight file fingerprint metadata."""

    mtime: float
    size: int
    digest: str


class FileFingerprintCache:
    """Cache file hashes keyed by absolute path and (mtime, size)."""

    def __init__(self, cache: "Cache | FanoutCache") -> None:
        self._cache = cache

    def get_or_compute(self, path: Path) -> str:
        if not path.exists():
            return "missing"
        try:
            stat = path.stat()
        except OSError:
            return "missing"
        cache_key = path.resolve().as_posix()
        entry = self._cache.get(cache_key, default=None)
        if isinstance(entry, dict):
            if entry.get("mtime") == stat.st_mtime and entry.get("size") == stat.st_size:
                digest = entry.get("digest")
                if isinstance(digest, str):
                    return digest
        digest = _hash_file(path)
        payload = {"mtime": stat.st_mtime, "size": stat.st_size, "digest": digest}
        self._cache.set(cache_key, payload, tag="cq_fingerprint", retry=True)
        return digest

    def invalidate(self, path: Path) -> bool:
        cache_key = path.resolve().as_posix()
        return bool(self._cache.delete(cache_key, retry=True))


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()[:16]


__all__ = [
    "FileFingerprint",
    "FileFingerprintCache",
]
