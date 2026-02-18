"""Tests for search artifact index path helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.policy import default_cache_policy
from tools.cq.core.cache.search_artifact_index import (
    global_index_path,
    global_order_path,
    open_artifact_deque,
    open_artifact_index,
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


def test_open_artifact_backends_create_parent_dirs_when_available(tmp_path: Path) -> None:
    """Open helpers should create parent directories when diskcache is present."""
    index_path = tmp_path / "index" / "global"
    deque_path = tmp_path / "deque" / "global_order"

    index_store = open_artifact_index(index_path)
    deque_store = open_artifact_deque(deque_path)

    if index_store is not None:
        assert index_path.parent.exists()
    if deque_store is not None:
        assert deque_path.parent.exists()
