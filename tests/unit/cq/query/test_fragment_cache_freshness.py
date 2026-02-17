"""Tests for test_fragment_cache_freshness."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend
from tools.cq.core.cache.telemetry import reset_cache_telemetry, snapshot_cache_telemetry
from tools.cq.query.executor_ast_grep import execute_ast_grep_rules as _execute_ast_grep_rules
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


@pytest.fixture(autouse=True)
def _cache_env(tmp_path: Path) -> Generator[None]:
    close_cq_cache_backend()
    reset_cache_telemetry()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    yield
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def test_pattern_fragment_cache_uses_content_hash_for_freshness(tmp_path: Path) -> None:
    """Use content hash changes to drive cache misses for pattern fragments."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    file_path = root / "module.py"
    file_path.write_text(
        "def target_symbol(x: int) -> int:\n    return x\n\nvalue = target_symbol(1)\n",
        encoding="utf-8",
    )

    query = parse_query("pattern='target_symbol($$$)' lang=python")
    plan = compile_query(query)
    scope_globs = None

    _execute_ast_grep_rules(
        plan.sg_rules,
        [file_path],
        root,
        query,
        scope_globs,
        run_id="run-a",
    )
    metrics_first = snapshot_cache_telemetry().get("pattern_fragment")

    file_stat = file_path.stat()
    os.utime(file_path, ns=(file_stat.st_atime_ns + 1_000_000, file_stat.st_mtime_ns + 1_000_000))

    _execute_ast_grep_rules(
        plan.sg_rules,
        [file_path],
        root,
        query,
        scope_globs,
        run_id="run-b",
    )
    metrics_second = snapshot_cache_telemetry().get("pattern_fragment")

    assert metrics_first is not None
    assert metrics_second is not None
    assert metrics_second.hits > metrics_first.hits
    assert metrics_second.misses == metrics_first.misses

    file_path.write_text(
        "def target_symbol(x: int) -> int:\n    return x + 1\n\nvalue = target_symbol(2)\n",
        encoding="utf-8",
    )

    _execute_ast_grep_rules(
        plan.sg_rules,
        [file_path],
        root,
        query,
        scope_globs,
        run_id="run-c",
    )
    metrics_third = snapshot_cache_telemetry().get("pattern_fragment")

    assert metrics_third is not None
    assert metrics_third.misses > metrics_second.misses
