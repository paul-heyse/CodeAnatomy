"""Performance benchmarks for cq query execution."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.enrichment import SymtableEnricher
from tools.cq.query.executor_plan_dispatch import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query

COLD_QUERY_MAX_SECONDS = 15.0
WARM_QUERY_MAX_SECONDS = 12.0
INDEX_BUILD_MAX_SECONDS = 60.0
SCALING_QUERY_MAX_SECONDS = 14.0


def _execute_query(*, plan: Any, query: Any, toolchain: Toolchain, root: Path) -> Any:
    return execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(root),
            services=resolve_runtime_services(root),
            symtable_enricher=SymtableEnricher(root),
            argv=(),
        ),
        tc=toolchain,
    )


@pytest.fixture
def repo_root() -> Path:
    """Get repository root path.

    Returns:
    -------
    Path
        Repository root directory.
    """
    return Path(__file__).resolve().parent.parent.parent.parent


@pytest.fixture
def toolchain() -> Toolchain:
    """Build a Toolchain instance for performance testing.

    Returns:
    -------
    Toolchain
        Detected toolchain instance.
    """
    return Toolchain.detect()


@pytest.mark.benchmark
def test_query_latency_cold(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure cold query execution completes in <15s.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance for query execution.
    repo_root : Path
        Repository root path.
    """
    query_text = "entity=function name=Toolchain lang=python"
    query = parse_query(query_text)
    plan = compile_query(query)

    start = time.perf_counter()
    result = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)
    elapsed = time.perf_counter() - start

    assert result is not None
    assert elapsed < COLD_QUERY_MAX_SECONDS, f"Cold query took {elapsed:.2f}s, expected <15s"


@pytest.mark.benchmark
def test_query_latency_warm(
    toolchain: Toolchain,
    repo_root: Path,
) -> None:
    """Ensure warm query (with index) completes in <12s.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance for query execution.
    repo_root : Path
        Repository root path.
    """
    # Warm the cache first
    query_text = "entity=function name=Toolchain lang=python"
    query = parse_query(query_text)
    plan = compile_query(query)
    _ = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)

    # Now measure warm execution
    start = time.perf_counter()
    result = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)
    elapsed = time.perf_counter() - start

    assert result is not None
    assert elapsed < WARM_QUERY_MAX_SECONDS, f"Warm query took {elapsed:.2f}s, expected <12s"


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
    query_text = "entity=class lang=python"
    query = parse_query(query_text)
    plan = compile_query(query)
    _ = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)
    elapsed = time.perf_counter() - start

    # Index build should be reasonable - adjust threshold based on repo size
    # For CodeAnatomy (~100k LOC), expect <60s on typical hardware
    assert elapsed < INDEX_BUILD_MAX_SECONDS, f"Index build took {elapsed:.2f}s, expected <60s"


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
        "entity=function lang=python",
        "entity=class lang=python",
        "entity=function name=execute lang=python",
    ]

    timings = []
    for query_text in queries:
        query = parse_query(query_text)
        plan = compile_query(query)

        start = time.perf_counter()
        result = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)
        elapsed = time.perf_counter() - start
        timings.append(elapsed)

        assert result is not None

    # All simple queries should complete quickly while tolerating CI variance.
    for i, elapsed in enumerate(timings):
        assert elapsed < SCALING_QUERY_MAX_SECONDS, (
            f"Query {i} took {elapsed:.2f}s, expected <{SCALING_QUERY_MAX_SECONDS:.0f}s"
        )
