"""Shared worker policy for search pipeline parallel stages."""

from __future__ import annotations

MAX_SEARCH_CLASSIFY_WORKERS = 4


def resolve_search_worker_count(partition_count: int) -> int:
    """Resolve worker count for search classification parallelism.

    Returns:
        int: Worker count bounded by partition count and max-worker cap.
    """
    if partition_count <= 1:
        return 1
    return min(partition_count, MAX_SEARCH_CLASSIFY_WORKERS)


__all__ = [
    "MAX_SEARCH_CLASSIFY_WORKERS",
    "resolve_search_worker_count",
]
