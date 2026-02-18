"""Tests for test_cache_maintenance."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache.backend_core import close_cq_cache_backend, get_cq_cache_backend
from tools.cq.core.cache.maintenance import maintenance_tick


def test_cache_maintenance_tick_reports_counters(tmp_path: Path) -> None:
    """Exercise maintenance counters and ensure telemetry fields are populated."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    backend = get_cq_cache_backend(root=tmp_path)
    assert backend.set("cq:maintenance:hit", 1, expire=60) is True
    assert backend.get("cq:maintenance:hit") == 1
    assert backend.get("cq:maintenance:miss") is None

    snapshot = maintenance_tick(backend)
    assert snapshot.hits >= 0
    assert snapshot.misses >= 0
    assert snapshot.expired_removed >= 0
    assert snapshot.culled_removed >= 0
    assert snapshot.integrity_errors >= 0

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
