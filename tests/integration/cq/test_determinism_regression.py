"""Determinism regression tests for CQ query execution.

Ensures that repeated query runs produce identical summary payloads,
which is critical for caching and reproducibility guarantees.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


@pytest.mark.integration
def test_repeated_runs_produce_identical_summary_payload() -> None:
    """Test that repeated query runs produce identical summary payloads.

    This regression test ensures determinism across multiple invocations of
    the same query, which is critical for:
    - Cache correctness (same input -> same cache key)
    - Reproducible builds
    - Debugging consistency
    """
    # Detect available toolchain
    tc = Toolchain.detect()
    services = resolve_runtime_services(Path())

    # Parse a simple query
    query = parse_query("entity=function in=src/relspec")

    # Compile the query into a plan
    plan = compile_query(query)

    # Execute the same plan twice with identical inputs
    request = ExecutePlanRequestV1(
        plan=plan,
        query=query,
        root=".",
        services=services,
        argv=("cq", "q", "entity=function in=src/relspec"),
    )

    result1 = execute_plan(request, tc=tc)
    result2 = execute_plan(request, tc=tc)

    # Extract summaries
    summary1 = result1.summary
    summary2 = result2.summary

    # Compare core summary fields (ignoring timestamps and cache backend stats)
    stable_keys = [
        "query",
        "mode",
        "total_defs",
        "total_calls",
        "matches",
        "files_scanned",
    ]

    for key in stable_keys:
        if key in summary1 and key in summary2:
            assert summary1[key] == summary2[key], (
                f"Summary field {key!r} differs: {summary1[key]} != {summary2[key]}"
            )

    # Verify finding counts are identical
    assert len(result1.key_findings) == len(result2.key_findings), (
        "Finding counts differ across runs"
    )

    # Verify section counts are identical
    assert len(result1.sections) == len(result2.sections), "Section counts differ across runs"


@pytest.mark.integration
def test_pattern_query_determinism() -> None:
    """Test determinism for pattern-based queries.

    Pattern queries use ast-grep rules and should also be deterministic.
    """
    tc = Toolchain.detect()
    services = resolve_runtime_services(Path())

    # Parse a pattern query
    query = parse_query("pattern='def $F($$$)' in=src/relspec")

    # Compile the query
    plan = compile_query(query)

    # Execute twice
    request = ExecutePlanRequestV1(
        plan=plan,
        query=query,
        root=".",
        services=services,
        argv=("cq", "q", "pattern='def $F($$$)' in=src/relspec"),
    )

    result1 = execute_plan(request, tc=tc)
    result2 = execute_plan(request, tc=tc)

    # Extract summaries
    summary1 = result1.summary
    summary2 = result2.summary

    # Compare stable fields
    stable_keys = ["query", "mode", "matches", "files_scanned"]

    for key in stable_keys:
        if key in summary1 and key in summary2:
            assert summary1[key] == summary2[key], (
                f"Summary field {key!r} differs: {summary1[key]} != {summary2[key]}"
            )

    # Verify finding counts are identical
    assert len(result1.key_findings) == len(result2.key_findings), (
        "Pattern query finding counts differ across runs"
    )
