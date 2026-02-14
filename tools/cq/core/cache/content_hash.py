"""Deterministic file content hashing helpers with process-local memoization."""

from __future__ import annotations

import hashlib
import threading
from pathlib import Path

from tools.cq.core.structs import CqStruct


class FileContentHashV1(CqStruct, frozen=True):
    """Stable file content hash payload."""

    digest: str
    size_bytes: int
    mtime_ns: int


_CONTENT_HASH_LOCK = threading.Lock()
_CONTENT_HASH_CACHE: dict[tuple[str, int, int], str] = {}
_MAX_CONTENT_HASH_CACHE = 16_384


def file_content_hash(file_path: Path) -> FileContentHashV1:
    """Return deterministic SHA256 digest for file content.

    Returns:
        Content hash payload using a lightweight process-local memo.
    """
    try:
        stat = file_path.stat()
        size_bytes = max(0, int(stat.st_size))
        mtime_ns = max(0, int(stat.st_mtime_ns))
    except (OSError, RuntimeError, ValueError):
        return FileContentHashV1(digest="", size_bytes=0, mtime_ns=0)

    key = (str(file_path.resolve()), size_bytes, mtime_ns)
    with _CONTENT_HASH_LOCK:
        cached = _CONTENT_HASH_CACHE.get(key)
        if cached is not None:
            return FileContentHashV1(
                digest=cached,
                size_bytes=size_bytes,
                mtime_ns=mtime_ns,
            )

    try:
        payload = file_path.read_bytes()
    except (OSError, RuntimeError, ValueError):
        return FileContentHashV1(digest="", size_bytes=size_bytes, mtime_ns=mtime_ns)
    digest = hashlib.sha256(payload).hexdigest()

    with _CONTENT_HASH_LOCK:
        if len(_CONTENT_HASH_CACHE) >= _MAX_CONTENT_HASH_CACHE:
            _CONTENT_HASH_CACHE.clear()
        _CONTENT_HASH_CACHE[key] = digest
    return FileContentHashV1(
        digest=digest,
        size_bytes=size_bytes,
        mtime_ns=mtime_ns,
    )


def reset_file_content_hash_cache() -> None:
    """Reset process-local content hash memo cache."""
    with _CONTENT_HASH_LOCK:
        _CONTENT_HASH_CACHE.clear()


__all__ = [
    "FileContentHashV1",
    "file_content_hash",
    "reset_file_content_hash_cache",
]
