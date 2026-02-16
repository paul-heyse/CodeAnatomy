"""Tests for test_cache_coordination."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache import (
    NoopCacheBackend,
    close_cq_cache_backend,
    get_cq_cache_backend,
    publish_once_per_barrier,
    tree_sitter_lane_guard,
)


def test_lane_guard_noop_backend() -> None:
    """Noop backend should still honor semaphore-lane guard entry/exit."""
    backend = NoopCacheBackend()
    marker = {"entered": False}
    with tree_sitter_lane_guard(
        backend=backend,
        lock_key="cq:test:lock",
        semaphore_key="cq:test:sema",
        lane_limit=1,
        ttl_seconds=5,
    ):
        marker["entered"] = True
    assert marker["entered"]



def test_publish_once_barrier_executes_function() -> None:
    """Publish-once barrier executes callback exactly once."""
    backend = NoopCacheBackend()
    counter = {"value": 0}

    def _publish() -> None:
        counter["value"] += 1

    publish_once_per_barrier(
        backend=backend,
        barrier_key="cq:test:publish",
        publish_fn=_publish,
    )
    assert counter["value"] == 1



def test_lane_guard_with_diskcache_backend(tmp_path: Path) -> None:
    """Protect diskcache lane updates with shared lock/semaphore coordination."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    backend = get_cq_cache_backend(root=tmp_path)
    with tree_sitter_lane_guard(
        backend=backend,
        lock_key="cq:test:lock",
        semaphore_key="cq:test:sema",
        lane_limit=2,
        ttl_seconds=5,
    ):
        assert backend.set("cq:test:key", {"ok": True}, expire=30) is True

    assert backend.get("cq:test:key") == {"ok": True}

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
