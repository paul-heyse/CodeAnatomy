"""Tests for cache backend registry."""

from __future__ import annotations

from tools.cq.core.cache.backend_registry import BackendRegistry
from tools.cq.core.cache.interface import NoopCacheBackend


def test_backend_registry_set_get_pop_clear() -> None:
    """Registry should support set/get/pop/clear lifecycle operations."""
    registry = BackendRegistry()
    backend = NoopCacheBackend()

    assert registry.get("ws") is None
    previous = registry.set("ws", backend)

    assert previous is None
    assert registry.get("ws") is backend

    popped = registry.pop("ws")
    assert popped is backend
    assert registry.get("ws") is None

    registry.set("a", NoopCacheBackend())
    registry.set("b", NoopCacheBackend())
    registry.clear()
    assert registry.values() == []
