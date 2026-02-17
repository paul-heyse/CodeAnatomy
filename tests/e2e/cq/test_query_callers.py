"""E2E tests for cq caller graph queries.

Tests the query command's ability to find callers of functions and methods.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult, Finding


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_callers_of_build_graph_product(
    run_query: Callable[[str], CqResult],
    assert_finding_exists: Callable[..., Finding],
) -> None:
    """Find callers of build_graph_product function.

    Tests that the query engine can:
    1. Locate the target function
    2. Return proper file/line anchors
    """
    result = run_query("entity=function name=build_graph_product fields=def,callers")

    # Should find the function itself
    func_finding = assert_finding_exists(
        result,
        category="definition",
        message_contains="build_graph_product",
        file="product_build.py",
    )
    assert func_finding.anchor is not None
    assert func_finding.anchor.line > 0

    # Query should complete successfully
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_callers_with_depth(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test depth parameter for transitive caller search.

    Verifies that the depth parameter is accepted and the query executes.
    """
    # Query with depth=1
    result_d1 = run_query(
        "entity=function name=build_graph_product expand=callers(depth=1) fields=def,callers"
    )

    findings_d1 = list(result_d1.key_findings + result_d1.evidence)
    for section in result_d1.sections:
        findings_d1.extend(section.findings)

    # Should find the function definition
    definitions = [f for f in findings_d1 if f.category == "definition"]
    assert len(definitions) > 0, "Expected to find the function definition"

    # Query with depth=2 (should also work)
    result_d2 = run_query(
        "entity=function name=build_graph_product expand=callers(depth=2) fields=def,callers"
    )

    # Both queries should complete successfully
    assert result_d1.run.macro == "q"
    assert result_d2.run.macro == "q"


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_callers_in_scope(
    run_query: Callable[[str], CqResult],
    assert_finding_exists: Callable[..., Finding],
) -> None:
    """Test scoped caller search.

    Verifies that the 'in=' scope parameter correctly filters results
    to only include files within the specified directory.
    """
    # Query with scope limited to src/graph/
    result = run_query("entity=function name=build_graph_product fields=def,callers in=src/graph/")

    # Collect all findings
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    # All findings should be in src/graph/ directory
    for finding in all_findings:
        if finding.anchor is not None:
            assert "src/graph/" in finding.anchor.file, (
                f"Finding at {finding.anchor.file} not in src/graph/ scope"
            )

    # Should still find the definition
    assert_finding_exists(
        result,
        category="definition",
        file="src/graph/product_build.py",
    )


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_callers_of_nonexistent_function(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test caller query for a function that does not exist.

    Should return empty results without error.
    """
    result = run_query("entity=function name=this_function_does_not_exist_xyz expand=callers")

    # Collect all non-diagnostic findings
    all_findings = [
        f
        for f in result.key_findings + result.evidence
        if f.category not in {"capability_limitation", "cross_language_hint"}
    ]
    for section in result.sections:
        all_findings.extend(
            f
            for f in section.findings
            if f.category not in {"capability_limitation", "cross_language_hint"}
        )

    # Should have no entity findings (capability diagnostics are expected)
    assert len(all_findings) == 0, "Expected no entity findings for nonexistent function"

    # Run should still be valid
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_callers_multiple_matches(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test caller query when multiple functions match the name.

    Common function names like 'parse' or 'build' may have multiple definitions.
    The query should find all matching definitions.
    """
    # 'parse' is a common function name prefix that appears multiple times
    result = run_query("entity=function name=~parse fields=def,callers")

    # Collect all findings
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find at least some results (many modules have 'parse' functions)
    definitions = [f for f in all_findings if f.category == "definition"]

    # May find multiple definitions
    assert len(definitions) > 0, "Expected at least one definition matching 'parse'"

    # If there are definitions, verify query completed
    assert result.run.elapsed_ms >= 0
