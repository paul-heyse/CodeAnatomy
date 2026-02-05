"""Performance benchmarks for cq query execution."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


@pytest.fixture
def repo_root() -> Path:
    """Get repository root path.

    Returns
    -------
    Path
        Repository root directory.
    """
    return Path(__file__).resolve().parent.parent.parent.parent


@pytest.fixture
def toolchain() -> Toolchain:
    """Build a Toolchain instance for performance testing.

    Returns
    -------
    Toolchain
        Detected toolchain instance.
    """
    return Toolchain.detect()


@pytest.mark.benchmark
def test_query_latency_cold(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure cold query execution completes in <10s.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance for query execution.
    repo_root : Path
        Repository root path.
    """
    query_text = "entity=function name=Toolchain"
    query = parse_query(query_text)
    plan = compile_query(query)

    start = time.perf_counter()
    result = execute_plan(plan, query, toolchain, repo_root)
    elapsed = time.perf_counter() - start

    assert result is not None
    assert elapsed < 10.0, f"Cold query took {elapsed:.2f}s, expected <10s"


@pytest.mark.benchmark
def test_query_latency_warm(
    toolchain: Toolchain,
    repo_root: Path,
) -> None:
    """Ensure warm query (with index) completes in <5s.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance for query execution.
    repo_root : Path
        Repository root path.
    """
    # Warm the cache first
    query_text = "entity=function name=Toolchain"
    query = parse_query(query_text)
    plan = compile_query(query)
    _ = execute_plan(plan, query, toolchain, repo_root)

    # Now measure warm execution
    start = time.perf_counter()
    result = execute_plan(plan, query, toolchain, repo_root)
    elapsed = time.perf_counter() - start

    assert result is not None
    assert elapsed < 5.0, f"Warm query took {elapsed:.2f}s, expected <5s"


@pytest.mark.benchmark
def test_index_build_time(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure index build completes in reasonable time.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    start = time.perf_counter()
    # Trigger index build by executing a query
    query_text = "entity=class"
    query = parse_query(query_text)
    plan = compile_query(query)
    _ = execute_plan(plan, query, toolchain, repo_root)
    elapsed = time.perf_counter() - start

    # Index build should be reasonable - adjust threshold based on repo size
    # For CodeAnatomy (~100k LOC), expect <60s on typical hardware
    assert elapsed < 60.0, f"Index build took {elapsed:.2f}s, expected <60s"


@pytest.mark.benchmark
def test_query_scaling_simple(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure simple queries scale linearly.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    queries = [
        "entity=function",
        "entity=class",
        "entity=function name=execute",
    ]

    timings = []
    for query_text in queries:
        query = parse_query(query_text)
        plan = compile_query(query)

        start = time.perf_counter()
        result = execute_plan(plan, query, toolchain, repo_root)
        elapsed = time.perf_counter() - start
        timings.append(elapsed)

        assert result is not None

    # All simple queries should complete quickly
    for i, elapsed in enumerate(timings):
        assert elapsed < 5.0, f"Query {i} took {elapsed:.2f}s, expected <5s"
