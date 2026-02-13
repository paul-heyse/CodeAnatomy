from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import cmd_calls
from tools.cq.query.executor import execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.search.smart_search import smart_search


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
    if not _benchmark_enabled():
        pytest.skip("Set CQ_ENABLE_BENCHMARK_SMOKE=1 to run benchmark smoke tests.")
    root = _golden_workspace()
    tc = Toolchain.detect()
    query = parse_query("entity=function name=build_graph lang=auto")
    plan = compile_query(query)

    start = time.perf_counter()
    execute_plan(plan, query, tc=tc, root=root, argv=[])
    first = time.perf_counter() - start

    start = time.perf_counter()
    execute_plan(plan, query, tc=tc, root=root, argv=[])
    second = time.perf_counter() - start
    assert second <= first * 1.5
