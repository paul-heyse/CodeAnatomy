"""Search object view registry for run-scoped view storage."""

from __future__ import annotations

from tools.cq.search.objects.render import SearchObjectResolvedViewV1
from tools.cq.search.pipeline.object_view_registry import (
    get_default_search_object_view_registry,
)


def register_search_object_view(
    *,
    run_id: str | None,
    view: SearchObjectResolvedViewV1,
) -> None:
    """Register a search object view for a run.

    Parameters
    ----------
    run_id
        Run identifier to associate with the view.
    view
        Resolved object view to store.
    """
    if not isinstance(run_id, str) or not run_id:
        return
    get_default_search_object_view_registry().register(run_id, view)


def pop_search_object_view_for_run(run_id: str | None) -> SearchObjectResolvedViewV1 | None:
    """Pop object-resolved search view payload for a completed run.

    Parameters
    ----------
    run_id
        Run identifier to retrieve view for.

    Returns:
    -------
    SearchObjectResolvedViewV1 | None
        Popped view if run_id exists and is cached, otherwise None.
    """
    if not isinstance(run_id, str) or not run_id:
        return None
    return get_default_search_object_view_registry().pop(run_id)


def clear_search_object_views() -> None:
    """Clear all run-scoped object views from default registry."""
    get_default_search_object_view_registry().clear()


__all__ = [
    "clear_search_object_views",
    "pop_search_object_view_for_run",
    "register_search_object_view",
]
