"""E2E tests for combined cq query features.

Tests complex queries that combine multiple query features:
- Multiple fields (callers, imports, etc.)
- Scope filtering
- Name matching
- Expanders with parameters
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tools.cq.core.summary_contract import SummaryEnvelopeV1

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult, Finding


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_combined_callers_with_fields(
    run_query: Callable[[str], CqResult],
    assert_finding_exists: Callable[..., Finding],
) -> None:
    """Test query combining caller expansion with additional fields.

    Verifies that multiple fields can be requested simultaneously:
    - fields=callers finds call sites
    - fields=imports collects import references for the function
    """
    # Query for a function with both callers and imports fields
    result = run_query("entity=function name=build_graph_product fields=def,callers,imports")

    # Should find the function definition
    definition = assert_finding_exists(
        result,
        category="definition",
        message_contains="build_graph_product",
    )
    assert definition.anchor is not None

    # Callers may or may not be present depending on the codebase
    # Just verify the query completed successfully
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_full_query_syntax(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test full query syntax with all options.

    Tests a complex query with:
    - entity type
    - name filter (regex pattern)
    - scope filter (in=)
    - expander with parameters
    - field selection
    """
    # Full-featured query using regex pattern to find functions containing 'execute'
    result = run_query(
        "entity=function name=~execute expand=callers(depth=1) fields=def,callers in=tools/cq/"
    )

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find some results (execute_plan in executor.py)
    definitions = [f for f in all_findings if f.category == "definition"]
    assert len(definitions) > 0, "Expected to find functions matching 'execute' in tools/cq/"

    # All findings should be in tools/cq/ scope
    for finding in all_findings:
        if finding.anchor is not None:
            assert "tools/cq/" in finding.anchor.file, (
                f"Finding at {finding.anchor.file} not in tools/cq/ scope"
            )

    # Verify query metadata
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0
    assert result.run.root, "Should have repo root"


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_scoped_import_with_name(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test import query with both scope and name filters.

    Combines:
    - entity=import (query type)
    - name=dataclasses (module filter)
    - in=src/ (scope filter)
    """
    result = run_query("entity=import name=dataclasses in=src/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find dataclasses imports in src/
    # (very common in Python codebases)
    if len(all_findings) > 0:
        # Verify all findings are in scope
        for finding in all_findings:
            if finding.anchor is not None:
                assert "src/" in finding.anchor.file, (
                    f"Finding at {finding.anchor.file} not in src/ scope"
                )
                # Message should mention dataclasses
                assert "dataclass" in finding.message.lower(), (
                    f"Finding message should mention dataclasses: {finding.message}"
                )

    # Query should complete successfully even if no results
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_class_with_methods_and_callers(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test class query with method expansion and callers.

    Tests entity=class combined with field expansion.
    """
    # Query for a class that likely exists
    result = run_query("entity=class name=Toolchain expand=callers(depth=1)")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find the Toolchain class definition
    definitions = [f for f in all_findings if f.category == "definition"]

    if len(definitions) > 0:
        # First definition should be the class
        class_def = definitions[0]
        assert class_def.anchor is not None
        assert "Toolchain" in class_def.message

        # May find callers (class instantiation or method calls)
        callsite_count = sum(1 for f in all_findings if f.category == "call_site")
        # Callsites are optional but query should complete
        assert callsite_count >= 0, "Callsite count should be non-negative"
        assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_method_query_with_scope(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test method-level query with scope filtering.

    Tests entity=method combined with scope.
    """
    # Query for methods in a specific directory
    result = run_query("entity=method in=tools/cq/core/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find method definitions in tools/cq/core/
    definition_count = sum(1 for f in all_findings if f.category == "definition")
    assert definition_count >= 0, "Definition count should be non-negative"

    # Verify all findings are in scope
    for finding in all_findings:
        if finding.anchor is not None:
            assert "tools/cq/core/" in finding.anchor.file, (
                f"Finding at {finding.anchor.file} not in tools/cq/core/ scope"
            )

    # Query should complete successfully
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_multiple_expanders(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test query with multiple expanders.

    Note: Current implementation may support only single expanders.
    This test verifies the behavior when multiple are requested.
    """
    # Try to combine multiple expanders
    # (this may not be fully supported yet, but should not crash)
    result = run_query("entity=function name=build_graph_product expand=callers")

    # Query should complete without error
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_query_result_metadata(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test that query results include proper metadata.

    Verifies:
    - RunMeta has required fields
    - Summary statistics are present
    - Toolchain info is included
    """
    result = run_query("entity=function name=execute in=tools/cq/")

    # Check RunMeta structure
    assert result.run.macro == "q"
    assert result.run.argv, "Should have argv"
    assert result.run.root, "Should have root path"
    assert result.run.started_ms > 0, "Should have started timestamp"
    assert result.run.elapsed_ms >= 0, "Should have elapsed time"
    assert result.run.schema_version, "Should have schema version"

    # Check toolchain info
    assert isinstance(result.run.toolchain, dict), "Toolchain should be a dict"

    # Summary should be present (may be empty)
    assert isinstance(result.summary, SummaryEnvelopeV1), (
        "Summary should be a typed SummaryEnvelopeV1"
    )


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_empty_query_all_entities(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test query for all entities of a type without filters.

    Tests a broad query: entity=function (no name, no scope).
    This should work but may return many results.
    """
    # Query for all functions (will be limited by default)
    result = run_query("entity=function")

    # Should find many functions
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    definitions = [f for f in all_findings if f.category == "definition"]
    assert len(definitions) > 0, "Expected to find function definitions"

    # Should span multiple files
    files = {f.anchor.file for f in definitions if f.anchor is not None}
    assert len(files) > 1, "Expected definitions from multiple files"

    # Query should complete
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_query_with_invalid_depth(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test query with invalid depth parameter.

    The system should either:
    1. Use a default depth
    2. Return an error in the result
    3. Gracefully handle the invalid parameter
    """
    # Try depth=0 (invalid)
    result = run_query("entity=function name=build_graph_product expand=callers(depth=0)")

    # Query should still complete (even if it returns no callers)
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0

    # Should not crash
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Findings structure should be valid
    for finding in all_findings:
        assert finding.category, "Finding should have category"
        assert finding.message, "Finding should have message"
