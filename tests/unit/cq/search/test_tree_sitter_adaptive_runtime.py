"""Tests for adaptive tree-sitter runtime helpers."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend
from tools.cq.search.tree_sitter.core.adaptive_runtime import (
    adaptive_query_budget_ms,
    record_runtime_sample,
    runtime_snapshot,
)

MIN_RECOMMENDED_BUDGET_MS = 25
MIN_RUNTIME_SAMPLES = 2


def test_adaptive_query_budget_uses_recorded_samples(tmp_path: Path) -> None:
    """Test adaptive query budget uses recorded samples."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    record_runtime_sample("python", 50.0)
    record_runtime_sample("python", 100.0)
    budget = adaptive_query_budget_ms(language="python", fallback_budget_ms=200)
    assert budget >= MIN_RECOMMENDED_BUDGET_MS
    snapshot = runtime_snapshot("python", fallback_budget_ms=200)
    assert snapshot.sample_count >= MIN_RUNTIME_SAMPLES
    assert snapshot.recommended_budget_ms == budget
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
