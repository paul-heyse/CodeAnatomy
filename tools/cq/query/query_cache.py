"""Shared cache read-through helpers for query scanners."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import msgspec

from tools.cq.core.cache.cache_decode import decode_cached_payload
from tools.cq.core.cache.interface import CqCacheBackend

_T = TypeVar("_T")


def cached_scan(
    *,
    root: Path,
    cache_key: str,
    scan_fn: Callable[[], _T],
    type_: type[_T],
    backend: CqCacheBackend | None,
    expire: int | None = None,
    tag: str | None = None,
) -> _T:
    """Execute scan with cache read-through/write-back semantics.

    Returns:
        _T: Cached or freshly scanned payload.
    """
    if backend is not None:
        cached = backend.get(cache_key)
        decoded, attempted = decode_cached_payload(
            root=root,
            backend=backend,
            payload=cached,
            type_=type_,
        )
        if attempted and decoded is not None:
            return decoded

    result = scan_fn()

    if backend is None:
        return result

    try:
        backend.set(
            cache_key,
            msgspec.msgpack.encode(result),
            expire=expire,
            tag=tag,
        )
    except (TypeError, ValueError, RuntimeError):
        # Cache write failures are non-fatal for query execution.
        return result

    return result


__all__ = ["cached_scan"]
