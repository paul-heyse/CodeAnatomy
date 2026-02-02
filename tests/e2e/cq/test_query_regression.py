"""Regression tests for cq query correctness."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.schema import CqResult
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
    """Build a Toolchain instance for testing.

    Returns
    -------
    Toolchain
        Detected toolchain instance.
    """
    return Toolchain.detect()


def _execute_query(query_text: str, toolchain: Toolchain, repo_root: Path) -> CqResult:
    """Execute a query and return results.

    Parameters
    ----------
    query_text : str
        Query string.
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.

    Returns
    -------
    CqResult
        Query execution result.
    """
    query = parse_query(query_text)
    plan = compile_query(query)
    return execute_plan(plan, query, toolchain, repo_root)


def test_regression_known_callers(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure known caller relationships are found.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query for execute_plan function which we know exists in query/executor.py
    result = _execute_query("entity=function name=execute_plan", toolchain, repo_root)

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) > 0, "Expected to find execute_plan function"

    # Verify we found the right function
    found_execute_plan = False
    for finding in all_findings:
        if finding.anchor and "execute_plan" in finding.message:
            found_execute_plan = True
            # Verify it's in the query module
            assert "query" in finding.anchor.file.lower()
            break

    assert found_execute_plan, "execute_plan function not found in results"


def test_regression_class_detection(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure classes are correctly detected.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query for Toolchain class which we know exists
    result = _execute_query("entity=class name=Toolchain", toolchain, repo_root)

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) > 0, "Expected to find Toolchain class"

    # Verify we found the Toolchain class
    found_toolchain = False
    for finding in all_findings:
        if finding.anchor and "Toolchain" in finding.message:
            found_toolchain = True
            # Verify it's in the core module
            assert "toolchain" in finding.anchor.file.lower()
            assert finding.category == "definition"
            break

    assert found_toolchain, "Toolchain class not found in results"


def test_regression_import_detection(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure imports are correctly detected.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query for pathlib imports which are common in the codebase
    result = _execute_query("entity=import name=Path", toolchain, repo_root)

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) > 0, "Expected to find Path imports"

    # Verify we found pathlib imports
    found_pathlib_import = False
    for finding in all_findings:
        if "Path" in finding.message:
            found_pathlib_import = True
            break

    assert found_pathlib_import, "Path imports not found in results"


def test_regression_scope_filtering(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure scope filtering works correctly.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query restricted to tools/cq directory
    result = _execute_query("entity=class in=tools/cq/", toolchain, repo_root)

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) > 0, "Expected to find classes in tools/cq/"

    # Verify all findings are within the specified scope
    for finding in all_findings:
        if finding.anchor:
            assert "tools/cq" in finding.anchor.file, (
                f"Finding {finding.message} not in scope: {finding.anchor.file}"
            )


def test_regression_name_pattern_matching(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure name pattern matching works correctly.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query for functions containing 'parse' (use ~ prefix for regex)
    result = _execute_query("entity=function name=~parse", toolchain, repo_root)

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) > 0, "Expected to find parse* functions"

    # Verify all findings match the pattern
    for finding in all_findings:
        message_lower = finding.message.lower()
        assert "parse" in message_lower, (
            f"Finding {finding.message} does not match pattern 'parse*'"
        )


def test_regression_multiple_filters(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure multiple filters work correctly together.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query with entity, name, and scope filters
    result = _execute_query(
        "entity=function name=compile* in=tools/cq/query/", toolchain, repo_root
    )

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Verify all findings match all filters
    for finding in all_findings:
        if finding.anchor:
            message_lower = finding.message.lower()
            assert "compile" in message_lower, (
                f"Finding {finding.message} does not contain 'compile'"
            )
            assert "tools/cq/query" in finding.anchor.file, (
                f"Finding {finding.message} not in scope: {finding.anchor.file}"
            )


def test_regression_empty_query_handling(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure queries with no matches return empty results gracefully.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query for something that definitely doesn't exist
    result = _execute_query("entity=class name=ThisClassDoesNotExistAnywhere", toolchain, repo_root)

    assert result is not None
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) == 0, "Expected empty results for non-existent class"
    assert result.run is not None
