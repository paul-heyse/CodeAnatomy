"""Regression tests for cq query correctness."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.schema import CqResult
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.enrichment import SymtableEnricher
from tools.cq.query.executor_plan_dispatch import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.search.pipeline.smart_search import smart_search


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

    Returns:
    -------
    CqResult
        Query execution result.
    """
    query = parse_query(query_text)
    plan = compile_query(query)
    return execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(repo_root),
            services=resolve_runtime_services(repo_root),
            symtable_enricher=SymtableEnricher(repo_root),
            argv=(),
            query_text=query_text,
        ),
        tc=toolchain,
    )


def test_regression_known_callers(toolchain: Toolchain, repo_root: Path) -> None:
    """Ensure known caller relationships are found.

    Parameters
    ----------
    toolchain : Toolchain
        Toolchain instance.
    repo_root : Path
        Repository root path.
    """
    # Query for execute_plan function in query/executor_plan_dispatch.py
    result = _execute_query("entity=function name=execute_plan", toolchain, repo_root)

    assert result is not None
    all_findings = list(result.key_findings + result.evidence)
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
    all_findings = list(result.key_findings + result.evidence)
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
    all_findings = list(result.key_findings + result.evidence)
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
    all_findings = list(result.key_findings + result.evidence)
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
    all_findings = list(result.key_findings + result.evidence)
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
    all_findings = list(result.key_findings + result.evidence)
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
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    assert len(all_findings) == 0, "Expected empty results for non-existent class"
    assert result.run is not None


def test_q_and_search_share_multilang_summary_contract(
    toolchain: Toolchain,
    repo_root: Path,
) -> None:
    """Q and search should expose the same top-level multilang summary keys."""
    q_result = _execute_query("entity=class name=Toolchain lang=auto", toolchain, repo_root)
    search_result = smart_search(
        repo_root, "Toolchain", tc=toolchain, argv=["cq", "search", "Toolchain"]
    )

    for result in (q_result, search_result):
        assert result.summary["lang_scope"] == "auto"
        assert result.summary["language_order"] == ["python", "rust"]
        assert "languages" in result.summary
        assert "cross_language_diagnostics" in result.summary


def test_q_explicit_lang_summary_preserves_scope_and_query_text(
    toolchain: Toolchain,
    repo_root: Path,
) -> None:
    """Explicit language q queries should retain full query text and lang scope."""
    query_text = (
        "entity=function name=execute_plan lang=python in=tools/cq/query/executor_plan_dispatch.py"
    )
    query = parse_query(query_text)
    plan = compile_query(query)
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(repo_root),
            services=resolve_runtime_services(repo_root),
            symtable_enricher=SymtableEnricher(repo_root),
            argv=(),
            query_text=query_text,
        ),
        tc=toolchain,
    )

    assert result.summary["query"] == query_text
    assert result.summary["mode"] == "entity"
    assert result.summary["lang_scope"] == "python"
    assert result.summary["language_order"] == ["python"]


def test_search_lang_scope_filters_file_extensions(tmp_path: Path) -> None:
    """Rust scope should exclude Python findings and Python scope should exclude Rust."""
    (tmp_path / "mod.py").write_text("def classify_match():\n    return 1\n", encoding="utf-8")
    (tmp_path / "mod.rs").write_text("fn classify_match() -> i32 { 1 }\n", encoding="utf-8")

    rust_result = smart_search(tmp_path, "classify_match", lang_scope="rust")
    rust_files = [
        finding.anchor.file for finding in rust_result.evidence if finding.anchor is not None
    ]
    assert rust_files
    assert all(file.endswith(".rs") for file in rust_files)

    python_result = smart_search(tmp_path, "classify_match", lang_scope="python")
    python_files = [
        finding.anchor.file for finding in python_result.evidence if finding.anchor is not None
    ]
    assert python_files
    assert all(file.endswith((".py", ".pyi")) for file in python_files)
