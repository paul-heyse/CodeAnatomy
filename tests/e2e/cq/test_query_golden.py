"""Golden snapshot tests for cq query outputs."""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query

from tests.e2e.cq._support.goldens import assert_json_snapshot, load_golden_query_spec


def _execute_query(
    *,
    plan: Any,
    query: Any,
    toolchain: Toolchain,
    root: Path,
    query_text: str | None = None,
) -> Any:
    return execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(root),
            argv=(),
            query_text=query_text,
        ),
        tc=toolchain,
    )


@pytest.fixture(autouse=True)
def _stable_query_environment(tmp_path: Path) -> Generator[None]:
    """Keep query golden tests deterministic across suite execution order.

    Yields:
    ------
    None
        Control back to the test with deterministic CQ environment settings.
    """
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    os.environ["CQ_ENABLE_SEMANTIC_ENRICHMENT"] = "1"
    os.environ["CQ_CACHE_STATISTICS_ENABLED"] = "0"
    os.environ["CQ_CACHE_STATS_ENABLED"] = "0"
    yield
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
    os.environ.pop("CQ_ENABLE_SEMANTIC_ENRICHMENT", None)
    os.environ.pop("CQ_CACHE_STATISTICS_ENABLED", None)
    os.environ.pop("CQ_CACHE_STATS_ENABLED", None)


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
    """Build a Toolchain instance for testing.

    Returns:
    -------
    Toolchain
        Detected toolchain instance.
    """
    return Toolchain.detect()


def test_toolchain_class_golden(
    toolchain: Toolchain,
    repo_root: Path,
    *,
    update_golden: bool,
) -> None:
    """Ensure Toolchain class query matches golden snapshot.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    update_golden : bool
        Whether to update golden files.
    """
    query_text = "entity=class name=Toolchain in=tools/cq/core/toolchain.py"
    query = parse_query(query_text)
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)

    assert result is not None
    assert_json_snapshot("query_toolchain_class.json", result, update=update_golden)


def test_execute_plan_function_golden(
    toolchain: Toolchain,
    repo_root: Path,
    *,
    update_golden: bool,
) -> None:
    """Ensure execute_plan function query matches golden snapshot.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    update_golden : bool
        Whether to update golden files.
    """
    query_text = "entity=function name=execute_plan in=tools/cq/query/executor.py"
    query = parse_query(query_text)
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)

    assert result is not None
    assert_json_snapshot("query_execute_plan_function.json", result, update=update_golden)


def test_parse_imports_golden(
    toolchain: Toolchain,
    repo_root: Path,
    *,
    update_golden: bool,
) -> None:
    """Ensure Path import query matches golden snapshot.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    update_golden : bool
        Whether to update golden files.
    """
    query_text = "entity=import name=Path in=tools/cq/index/files.py"
    query = parse_query(query_text)
    plan = compile_query(query)
    result = _execute_query(plan=plan, query=query, toolchain=toolchain, root=repo_root)

    assert result is not None
    assert_json_snapshot("query_path_imports.json", result, update=update_golden)


def test_query_spec_toolchain_class() -> None:
    """Verify golden query spec structure for Toolchain class query.

    This test validates the structure of the golden query specification
    without executing the query.
    """
    spec = load_golden_query_spec("query_spec_toolchain_class.json")

    assert "query" in spec
    assert spec["query"] == "entity=class name=Toolchain in=tools/cq/"

    assert "expected_matches" in spec
    assert spec["expected_matches"] >= 1

    assert "expected_kind" in spec
    assert spec["expected_kind"] == "class"


def test_query_spec_execute_plan_function() -> None:
    """Verify golden query spec structure for execute_plan function query.

    This test validates the structure of the golden query specification
    without executing the query.
    """
    spec = load_golden_query_spec("query_spec_execute_plan_function.json")

    assert "query" in spec
    assert spec["query"] == "entity=function name=execute_plan in=tools/cq/query/"

    assert "expected_matches" in spec
    assert spec["expected_matches"] >= 1

    assert "expected_kind" in spec
    assert spec["expected_kind"] == "function"


def test_query_with_spec_validation(toolchain: Toolchain, repo_root: Path) -> None:
    """Execute query and validate against golden spec expectations.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    spec = load_golden_query_spec("query_spec_toolchain_class.json")

    query_text = spec["query"]
    query = parse_query(query_text)
    plan = compile_query(query)
    result = _execute_query(
        plan=plan,
        query=query,
        toolchain=toolchain,
        root=repo_root,
        query_text=query_text,
    )

    assert result is not None

    # Validate against spec expectations
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    total_findings = len(all_findings)
    assert total_findings >= spec["expected_matches"], (
        f"Expected at least {spec['expected_matches']} matches, got {total_findings}"
    )
