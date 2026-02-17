"""Tests for search artifact index path helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.search_artifact_index import (
    global_index_path,
    global_order_path,
    run_index_path,
    run_order_path,
)


def test_search_artifact_index_paths_are_under_store_root(tmp_path: Path) -> None:
    """All artifact index helper paths should resolve under cache store root."""
    policy = default_cache_policy(root=tmp_path)

    assert str(global_index_path(policy)).endswith("stores/search_artifacts/index/global")
    assert str(global_order_path(policy)).endswith("stores/search_artifacts/deque/global_order")
    assert str(run_index_path(policy, "abc")).endswith("stores/search_artifacts/index/run_abc")
    assert str(run_order_path(policy, "abc")).endswith("stores/search_artifacts/deque/run_abc")
