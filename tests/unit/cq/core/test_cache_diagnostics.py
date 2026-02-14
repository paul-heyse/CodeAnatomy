from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache import close_cq_cache_backend
from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.telemetry import (
    record_cache_abort,
    record_cache_get,
    record_cache_set,
    reset_cache_telemetry,
)


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
    assert search_metrics["gets"] == 2
    assert search_metrics["hits"] == 1
    assert search_metrics["misses"] == 1
    assert search_metrics["hit_ratio"] == 0.5
    assert search_metrics["miss_ratio"] == 0.5
    assert search_metrics["sets"] == 2
    assert search_metrics["set_failures"] == 1
    assert search_metrics["aborts"] == 1
    assert search_metrics["key_cardinality"] == 3
