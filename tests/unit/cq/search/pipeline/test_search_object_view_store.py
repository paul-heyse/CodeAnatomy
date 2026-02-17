"""Tests for run-scoped object-view store helpers."""

from __future__ import annotations

from tools.cq.search.objects.render import SearchObjectResolvedViewV1
from tools.cq.search.pipeline.object_view_registry import SearchObjectViewRegistry
from tools.cq.search.pipeline.search_object_view_store import (
    clear_search_object_views,
    pop_search_object_view_for_run,
    register_search_object_view,
)


def test_store_register_and_pop_with_injected_registry() -> None:
    """Store helpers should use explicit injected registry when provided."""
    registry = SearchObjectViewRegistry()
    view = SearchObjectResolvedViewV1()

    register_search_object_view(run_id="run-1", view=view, registry=registry)
    assert pop_search_object_view_for_run("run-1", registry=registry) == view
    assert pop_search_object_view_for_run("run-1", registry=registry) is None


def test_store_clear_with_injected_registry() -> None:
    """Clear helper should empty explicit injected registry."""
    registry = SearchObjectViewRegistry()
    register_search_object_view(
        run_id="run-1",
        view=SearchObjectResolvedViewV1(),
        registry=registry,
    )

    clear_search_object_views(registry=registry)

    assert pop_search_object_view_for_run("run-1", registry=registry) is None
