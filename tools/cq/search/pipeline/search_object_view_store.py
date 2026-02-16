"""Search object view registry for run-scoped view storage."""

from __future__ import annotations

from tools.cq.search.objects.render import SearchObjectResolvedViewV1

# Module-level registry for storing object views by run_id
_SEARCH_OBJECT_VIEW_REGISTRY: dict[str, SearchObjectResolvedViewV1] = {}


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
    _SEARCH_OBJECT_VIEW_REGISTRY[run_id] = view


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
    return _SEARCH_OBJECT_VIEW_REGISTRY.pop(run_id, None)


__all__ = [
    "pop_search_object_view_for_run",
    "register_search_object_view",
]
