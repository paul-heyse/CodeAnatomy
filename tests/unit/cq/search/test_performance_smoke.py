"""Tests for test_performance_smoke."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import cmd_calls
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.search.pipeline.smart_search import smart_search


def _benchmark_enabled() -> bool:
    return os.getenv("CQ_ENABLE_BENCHMARK_SMOKE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _golden_workspace() -> Path:
    return (
        Path(__file__).resolve().parents[3] / "e2e" / "cq" / "_golden_workspace" / "python_project"
    )


@pytest.mark.benchmark
def test_search_cache_smoke() -> None:
    """Benchmark expected repeated smart_search invocations stay performant."""
    if not _benchmark_enabled():
        pytest.skip("Set CQ_ENABLE_BENCHMARK_SMOKE=1 to run benchmark smoke tests.")
    root = _golden_workspace()
    tc = Toolchain.detect()

    start = time.perf_counter()
    smart_search(
        root=root,
        query="build_graph",
        lang_scope="python",
        tc=tc,
        argv=[],
    )
    first = time.perf_counter() - start

    start = time.perf_counter()
    smart_search(
        root=root,
        query="build_graph",
        lang_scope="python",
        tc=tc,
        argv=[],
    )
    second = time.perf_counter() - start
    assert second <= first * 1.5


@pytest.mark.benchmark
def test_calls_cache_smoke() -> None:
    """Benchmark repeated calls command executes with bounded regression."""
    if not _benchmark_enabled():
        pytest.skip("Set CQ_ENABLE_BENCHMARK_SMOKE=1 to run benchmark smoke tests.")
    root = _golden_workspace()
    tc = Toolchain.detect()

    start = time.perf_counter()
    cmd_calls(tc=tc, root=root, argv=[], function_name="build_graph")
    first = time.perf_counter() - start

    start = time.perf_counter()
    cmd_calls(tc=tc, root=root, argv=[], function_name="build_graph")
    second = time.perf_counter() - start
    assert second <= first * 1.5


@pytest.mark.benchmark
def test_query_entity_auto_scope_smoke() -> None:
    """Benchmark execute_plan path under auto scope remains stable on warm path."""
    if not _benchmark_enabled():
        pytest.skip("Set CQ_ENABLE_BENCHMARK_SMOKE=1 to run benchmark smoke tests.")
    root = _golden_workspace()
    tc = Toolchain.detect()
    query = parse_query("entity=function name=build_graph lang=auto")
    plan = compile_query(query)

    start = time.perf_counter()
    execute_plan(
        ExecutePlanRequestV1(plan=plan, query=query, root=str(root), argv=()),
        tc=tc,
    )
    first = time.perf_counter() - start

    start = time.perf_counter()
    execute_plan(
        ExecutePlanRequestV1(plan=plan, query=query, root=str(root), argv=()),
        tc=tc,
    )
    second = time.perf_counter() - start
    assert second <= first * 1.5
