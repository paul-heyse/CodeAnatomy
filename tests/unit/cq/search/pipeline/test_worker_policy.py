"""Tests for search worker policy helpers."""

from __future__ import annotations

from tools.cq.search.pipeline.worker_policy import (
    MAX_SEARCH_CLASSIFY_WORKERS,
    resolve_search_worker_count,
)


def test_resolve_search_worker_count_returns_one_for_single_partition() -> None:
    """Single partition inputs should use one worker."""
    assert resolve_search_worker_count(1) == 1


def test_resolve_search_worker_count_respects_max_cap() -> None:
    """Worker count should not exceed the configured classify-worker cap."""
    assert (
        resolve_search_worker_count(MAX_SEARCH_CLASSIFY_WORKERS + 10) == MAX_SEARCH_CLASSIFY_WORKERS
    )
