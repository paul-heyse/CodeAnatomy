"""Tests for execute_plan query cache integration."""

from __future__ import annotations

import sys
from pathlib import Path

from tools.cq.core.schema import mk_result, mk_runmeta, ms
from tools.cq.core.toolchain import Toolchain
from tools.cq.index.query_cache import QueryCache
from tools.cq.query.executor import _build_query_cache_key, _collect_cache_files, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


def _build_toolchain() -> Toolchain:
    """Build a minimal Toolchain instance for caching tests."""
    py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    return Toolchain(
        rg_path=None,
        rg_version=None,
        sg_path=None,
        sg_version=None,
        py_path=sys.executable,
        py_version=py_version,
    )


def test_execute_plan_uses_query_cache(tmp_path: Path) -> None:
    """execute_plan should return cached results when available."""
    root = tmp_path
    file_path = root / "sample.py"
    file_path.write_text("def foo() -> int:\n    return 1\n", encoding="utf-8")

    query = parse_query("entity=function name=foo")
    plan = compile_query(query)
    toolchain = _build_toolchain()

    cache_files = _collect_cache_files(plan, root)
    assert cache_files

    cache_key = _build_query_cache_key(query, plan, root, toolchain)
    cached_run = mk_runmeta("q", [], str(root), ms(), toolchain.to_dict())
    cached_result = mk_result(cached_run)
    cached_result.summary["note"] = "cached"

    cache_dir = root / "cache"
    with QueryCache(cache_dir) as query_cache:
        query_cache.set(cache_key, cached_result.to_dict(), cache_files)
        result = execute_plan(
            plan=plan,
            query=query,
            tc=toolchain,
            root=root,
            query_cache=query_cache,
            use_cache=True,
        )

    assert result.summary["note"] == "cached"
    cache_info = result.summary.get("cache", {})
    assert cache_info.get("status") == "hit"
