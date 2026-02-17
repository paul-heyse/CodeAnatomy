"""Tests for adaptive-runtime cache backend protocol compatibility."""

from __future__ import annotations

from tools.cq.search.tree_sitter.core.adaptive_runtime import memoized_value, record_runtime_sample


class _StubCache:
    def __init__(self) -> None:
        self.rows: dict[str, object] = {}

    def get(self, key: str) -> object | None:
        return self.rows.get(key)

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = (expire, tag)
        self.rows[key] = value
        return True


def test_memoized_value_uses_protocol_backend() -> None:
    """Verify memoization reads/writes through protocol-compatible backend."""
    cache = _StubCache()
    computed = memoized_value(key="demo", compute=lambda: "value", cache_backend=cache)
    cached = memoized_value(key="demo", compute=lambda: "other", cache_backend=cache)

    assert computed == "value"
    assert cached == "value"


def test_record_runtime_sample_writes_stats_via_protocol() -> None:
    """Verify runtime sample helper writes stats rows via cache protocol."""
    cache = _StubCache()
    record_runtime_sample("python", 12.5, cache_backend=cache)
    stats = cache.rows.get("tree_sitter:runtime_stats:python")
    assert isinstance(stats, dict)
    assert stats.get("count") == 1
