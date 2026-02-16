"""Tests for test_cache_diagnostics."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend
from tools.cq.core.cache.telemetry import (
    record_cache_abort,
    record_cache_get,
    record_cache_set,
    reset_cache_telemetry,
)

EXPECTED_GETS = 2
EXPECTED_HITS = 1
EXPECTED_MISSES = 1
EXPECTED_RATIO = 0.5
EXPECTED_SETS = 2
EXPECTED_SET_FAILURES = 1
EXPECTED_ABORTS = 1
EXPECTED_KEY_CARDINALITY = 3


@pytest.fixture(autouse=True)
def _reset_cache_state() -> Generator[None]:
    reset_cache_telemetry()
    close_cq_cache_backend()
    yield
    close_cq_cache_backend()
    reset_cache_telemetry()


def test_snapshot_backend_metrics_includes_namespace_hit_ratios(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Assert namespace-level hit and miss ratios appear in diagnostics payload."""
    monkeypatch.setenv("CQ_CACHE_ENABLED", "0")

    record_cache_get(namespace="search_candidates", hit=True, key="short_key")
    record_cache_get(namespace="search_candidates", hit=False, key="other_key")
    record_cache_set(namespace="search_candidates", ok=True, key="short_key")
    record_cache_set(namespace="search_candidates", ok=False, key="failing_key")
    record_cache_abort(namespace="search_candidates")

    payload = snapshot_backend_metrics(root=tmp_path)
    namespaces = payload.get("namespaces")
    assert isinstance(namespaces, dict)

    search_metrics = namespaces.get("search_candidates")
    assert isinstance(search_metrics, dict)
    assert search_metrics["gets"] == EXPECTED_GETS
    assert search_metrics["hits"] == EXPECTED_HITS
    assert search_metrics["misses"] == EXPECTED_MISSES
    assert search_metrics["hit_ratio"] == EXPECTED_RATIO
    assert search_metrics["miss_ratio"] == EXPECTED_RATIO
    assert search_metrics["sets"] == EXPECTED_SETS
    assert search_metrics["set_failures"] == EXPECTED_SET_FAILURES
    assert search_metrics["aborts"] == EXPECTED_ABORTS
    assert search_metrics["key_cardinality"] == EXPECTED_KEY_CARDINALITY
