"""Tests for fragment cache runtime helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from tools.cq.core.cache.fragment_runtime import probe_or_persist_fragment
from tools.cq.core.cache.interface import CqCacheBackend


@dataclass
class _Backend:
    store: dict[str, bytes]

    def get(self, key: str) -> bytes | None:
        return self.store.get(key)

    def set(
        self, key: str, value: bytes, expire: int | None = None, tag: str | None = None
    ) -> bool:
        _ = (expire, tag)
        self.store[key] = bytes(value)
        return True


def test_probe_or_persist_fragment_hits_cache_without_recompute() -> None:
    """Use cached payload when present and avoid recompute."""
    backend = _Backend(store={"k": b"cached"})

    calls = 0

    def _compute() -> bytes:
        nonlocal calls
        calls += 1
        return b"new"

    payload = probe_or_persist_fragment(
        backend=cast("CqCacheBackend", backend),
        cache_key="k",
        compute=_compute,
        ttl_seconds=10,
    )

    assert payload == b"cached"
    assert calls == 0


def test_probe_or_persist_fragment_computes_and_persists_on_miss() -> None:
    """Compute and persist payload when cache miss occurs."""
    backend = _Backend(store={})

    calls = 0

    def _compute() -> bytes:
        nonlocal calls
        calls += 1
        return b"new"

    payload = probe_or_persist_fragment(
        backend=cast("CqCacheBackend", backend),
        cache_key="k",
        compute=_compute,
        ttl_seconds=10,
    )

    assert payload == b"new"
    assert backend.store["k"] == b"new"
    assert calls == 1
