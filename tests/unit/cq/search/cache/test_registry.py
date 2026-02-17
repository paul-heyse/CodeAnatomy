"""Tests for unified search cache registry."""

from __future__ import annotations

from tools.cq.search.cache.registry import CacheRegistry


class _DummyCache:
    def __init__(self) -> None:
        self.clears = 0

    def clear(self) -> None:
        self.clears += 1


def test_clear_all_language_scope() -> None:
    """Language-scoped clear only touches matching caches/callbacks."""
    registry = CacheRegistry()
    rust_cache = _DummyCache()
    py_cache = _DummyCache()
    callback_hits = {"rust": 0, "python": 0}

    registry.register_cache("rust", "runtime", rust_cache)
    registry.register_cache("python", "analysis", py_cache)
    registry.register_clear_callback("rust", lambda: callback_hits.__setitem__("rust", 1))
    registry.register_clear_callback("python", lambda: callback_hits.__setitem__("python", 1))

    registry.clear_all("rust")

    assert rust_cache.clears == 1
    assert py_cache.clears == 0
    assert callback_hits["rust"] == 1
    assert callback_hits["python"] == 0


def test_clear_all_without_language() -> None:
    """Global clear touches all registered caches and callbacks."""
    registry = CacheRegistry()
    rust_cache = _DummyCache()
    py_cache = _DummyCache()
    callback_hits = {"rust": 0, "python": 0}

    registry.register_cache("rust", "runtime", rust_cache)
    registry.register_cache("python", "analysis", py_cache)
    registry.register_clear_callback("rust", lambda: callback_hits.__setitem__("rust", 1))
    registry.register_clear_callback("python", lambda: callback_hits.__setitem__("python", 1))

    registry.clear_all()

    assert rust_cache.clears == 1
    assert py_cache.clears == 1
    assert callback_hits["rust"] == 1
    assert callback_hits["python"] == 1
