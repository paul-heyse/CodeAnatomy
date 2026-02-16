"""Tests for test_smart_search_cache_matrix."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import msgspec
import pytest
from tools.cq.core.cache import (
    close_cq_cache_backend,
    reset_cache_telemetry,
    snapshot_cache_telemetry,
)
from tools.cq.search.pipeline.smart_search import smart_search


@pytest.fixture(autouse=True)
def _cache_env(tmp_path: Path) -> Generator[None]:
    close_cq_cache_backend()
    reset_cache_telemetry()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    os.environ["CQ_ENABLE_SEMANTIC_ENRICHMENT"] = "0"
    yield
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
    os.environ.pop("CQ_ENABLE_SEMANTIC_ENRICHMENT", None)


def _normalize_result_payload(result: object) -> dict[str, object]:
    payload = msgspec.to_builtins(result)
    if not isinstance(payload, dict):
        return {}
    run = payload.get("run")
    if isinstance(run, dict):
        run["started_ms"] = 0
        run["elapsed_ms"] = 0
    summary = payload.get("summary")
    if isinstance(summary, dict):
        summary.pop("cache_backend", None)
        summary.pop("search_stage_timings_ms", None)
    return payload


def test_smart_search_is_deterministic_across_reruns(tmp_path: Path) -> None:
    """Re-run smart search with stable inputs should yield deterministic payloads."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    (root / "module.py").write_text(
        "def target_symbol(value: int) -> int:\n    return value\n\nresult = target_symbol(1)\n",
        encoding="utf-8",
    )

    result_a = smart_search(
        root,
        "target_symbol",
        mode="identifier",
        lang_scope="python",
        run_id="deterministic-run",
    )
    result_b = smart_search(
        root,
        "target_symbol",
        mode="identifier",
        lang_scope="python",
        run_id="deterministic-run",
    )

    assert _normalize_result_payload(result_a) == _normalize_result_payload(result_b)


def test_smart_search_is_deterministic_across_backend_restarts(tmp_path: Path) -> None:
    """Result stability should survive backend restart for identical query inputs."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    (root / "module.py").write_text(
        "def target_symbol(value: int) -> int:\n    return value\n\nresult = target_symbol(1)\n",
        encoding="utf-8",
    )

    result_a = smart_search(
        root,
        "target_symbol",
        mode="identifier",
        lang_scope="python",
        run_id="deterministic-run",
    )
    close_cq_cache_backend(root=root)
    result_b = smart_search(
        root,
        "target_symbol",
        mode="identifier",
        lang_scope="python",
        run_id="deterministic-run",
    )

    assert _normalize_result_payload(result_a) == _normalize_result_payload(result_b)


def test_search_enrichment_reuses_unchanged_anchors_and_recomputes_changed_anchors(
    tmp_path: Path,
) -> None:
    """Recompute stale enrichments after source changes but keep unchanged anchors cached."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    first = root / "first.py"
    second = root / "second.py"
    first.write_text(
        "def target_symbol(value: int) -> int:\n    return value\n\nx = target_symbol(1)\n",
        encoding="utf-8",
    )
    second.write_text(
        "def use_symbol() -> int:\n    return target_symbol(2)\n",
        encoding="utf-8",
    )

    smart_search(root, "target_symbol", mode="identifier", lang_scope="python", run_id="run-1")
    first_metrics = snapshot_cache_telemetry().get("search_enrichment")

    first.write_text(
        "def target_symbol(value: int) -> int:\n    return value + 1\n\nx = target_symbol(3)\n",
        encoding="utf-8",
    )

    smart_search(root, "target_symbol", mode="identifier", lang_scope="python", run_id="run-2")
    second_metrics = snapshot_cache_telemetry().get("search_enrichment")

    assert first_metrics is not None
    assert second_metrics is not None
    assert second_metrics.hits > first_metrics.hits
    assert second_metrics.misses > first_metrics.misses
