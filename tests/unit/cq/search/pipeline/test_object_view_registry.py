"""Tests for search object view registry."""

from __future__ import annotations

from tools.cq.search.objects.render import SearchObjectResolvedViewV1
from tools.cq.search.pipeline.object_view_registry import SearchObjectViewRegistry


def test_object_view_registry_register_and_pop() -> None:
    """Object view registry should return and remove registered run views."""
    registry = SearchObjectViewRegistry()
    view = SearchObjectResolvedViewV1()

    registry.register("run-1", view)
    popped = registry.pop("run-1")

    assert popped == view
    assert registry.pop("run-1") is None


def test_object_view_registry_clear() -> None:
    """Object view registry clear should remove all registered views."""
    registry = SearchObjectViewRegistry()
    registry.register("run-1", SearchObjectResolvedViewV1())
    registry.register("run-2", SearchObjectResolvedViewV1())

    registry.clear()

    assert registry.pop("run-1") is None
    assert registry.pop("run-2") is None
