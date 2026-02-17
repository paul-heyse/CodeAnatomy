"""Thread-safe registry for run-scoped search object views."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from tools.cq.search.objects.render import SearchObjectResolvedViewV1


@dataclass
class SearchObjectViewRegistry:
    """Mutable run-id keyed registry for resolved object views."""

    _rows: dict[str, SearchObjectResolvedViewV1] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def register(self, run_id: str, view: SearchObjectResolvedViewV1) -> None:
        """Register or replace view for a run id."""
        if not run_id:
            return
        with self._lock:
            self._rows[run_id] = view

    def pop(self, run_id: str) -> SearchObjectResolvedViewV1 | None:
        """Pop and return view for run id if present.

        Returns:
            SearchObjectResolvedViewV1 | None: Removed view for `run_id`, when present.
        """
        if not run_id:
            return None
        with self._lock:
            return self._rows.pop(run_id, None)

    def clear(self) -> None:
        """Clear all registry rows."""
        with self._lock:
            self._rows.clear()


_DEFAULT_REGISTRY = SearchObjectViewRegistry()


def get_default_search_object_view_registry() -> SearchObjectViewRegistry:
    """Return process-wide default search object view registry.

    Returns:
        SearchObjectViewRegistry: Process-wide run-id keyed object-view registry.
    """
    return _DEFAULT_REGISTRY


__all__ = [
    "SearchObjectViewRegistry",
    "get_default_search_object_view_registry",
]
