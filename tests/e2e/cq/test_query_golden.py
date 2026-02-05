"""Golden snapshot tests for cq query outputs."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query

from tests.e2e.cq._support.goldens import assert_json_snapshot, load_golden_query_spec


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
    """Build a Toolchain instance for testing.

    Returns
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
    result = execute_plan(plan, query, toolchain, repo_root)

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
    result = execute_plan(plan, query, toolchain, repo_root)

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
    result = execute_plan(plan, query, toolchain, repo_root)

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
    result = execute_plan(plan, query, toolchain, repo_root)

    assert result is not None

    # Validate against spec expectations
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    total_findings = len(all_findings)
    assert total_findings >= spec["expected_matches"], (
        f"Expected at least {spec['expected_matches']} matches, got {total_findings}"
    )
