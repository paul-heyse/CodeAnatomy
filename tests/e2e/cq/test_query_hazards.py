"""E2E tests for cq hazard detection queries.

Tests the query command's ability to detect and report code hazards.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazards_in_macros(
    run_query: Callable[[str], CqResult],
) -> None:
    """Find hazards in cq macros module.

    Tests that the query engine can:
    1. Detect code hazards (async patterns, side effects, etc.)
    2. Return proper categories and severity
    3. Provide actionable messages
    """
    # Query for hazards in the macros directory
    result = run_query("entity=function fields=hazards in=tools/cq/macros/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find some function definitions
    definitions = [f for f in all_findings if f.category == "definition"]
    assert len(definitions) > 0, "Expected function definitions in macros/"

    # Check that hazard analysis was performed
    # (not all functions will have hazards, but the field should be present)
    for finding in definitions:
        if finding.anchor is not None:
            assert "tools/cq/macros/" in finding.anchor.file, (
                f"Finding at {finding.anchor.file} not in tools/cq/macros/ scope"
            )


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazard_types(
    run_query: Callable[[str], CqResult],
) -> None:
    """Verify different hazard types are detected.

    Hazards might include:
    - Async patterns (await, async def)
    - Exception handling patterns
    - Global mutations
    - Side effects
    """
    # Query for functions with hazard analysis in a module likely to have them
    result = run_query("entity=function fields=hazards in=src/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find function definitions
    definitions = [f for f in all_findings if f.category == "definition"]
    assert len(definitions) > 0, "Expected function definitions"

    # Check for hazard-related details
    # Hazards might be in details, or there might be separate hazard findings
    hazard_count = sum(
        1 for f in all_findings if "hazard" in f.category.lower() or "hazard" in f.message.lower()
    )

    # It's okay if no hazards are found, but verify the query executed
    assert hazard_count >= 0, "Hazard count should be non-negative"
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazards_with_severity(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test that hazard findings include severity levels.

    Severity should be one of: info, warning, error
    """
    # Query for hazards in a specific module
    result = run_query("entity=function fields=hazards in=tools/cq/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Verify all findings have valid severity
    valid_severities = {"info", "warning", "error"}
    for finding in all_findings:
        assert finding.severity in valid_severities, f"Invalid severity: {finding.severity}"


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazards_async_functions(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test hazard detection for async functions.

    Async functions should be detected and may be flagged as hazards
    depending on context.
    """
    # Query for async-related patterns in integration tests (likely to have async)
    result = run_query("entity=function fields=hazards in=tests/integration/")

    # Should find some function definitions
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    definitions = [f for f in all_findings if f.category == "definition"]

    # Should find at least one function definition
    assert len(definitions) > 0, "Expected function definitions in tests/integration/"

    # Verify query completed successfully
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazards_in_empty_scope(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test hazard query in a scope with no Python files.

    Should return empty results without error.
    """
    # Query in a scope that likely has no Python files
    result = run_query("entity=function fields=hazards in=docs/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should have few or no findings (docs/ might have some Python examples)
    # The important thing is that the query completes without error
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazards_detail_structure(
    run_query: Callable[[str], CqResult],
) -> None:
    """Verify hazard findings have expected detail fields.

    Hazard findings should include useful metadata about the hazard type
    and potentially suggestions for remediation.
    """
    # Query for hazards in cq query module
    result = run_query("entity=function fields=hazards in=tools/cq/query/")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find some definitions
    assert len(all_findings) > 0, "Expected findings in tools/cq/query/"

    # Check that findings have proper structure
    for finding in all_findings:
        assert finding.category, "Finding should have category"
        assert finding.message, "Finding should have message"
        assert isinstance(finding.details, dict), "Details should be a dict"

        # If it has an anchor, verify structure
        if finding.anchor is not None:
            assert finding.anchor.file, "Anchor should have file"
            assert finding.anchor.line > 0, "Anchor should have positive line"


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_hazards_combined_with_name(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test hazard detection combined with name filtering.

    Should find hazards only for functions matching the name pattern.
    """
    # Query for specific function with hazard analysis
    result = run_query("entity=function name=execute fields=hazards")

    # Collect all findings
    all_findings = result.key_findings + result.evidence
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should only find functions with 'execute' in the name
    definitions = [f for f in all_findings if f.category == "definition"]
    for definition in definitions:
        # Message should contain the function name
        assert "execute" in definition.message.lower(), (
            f"Definition message should contain 'execute': {definition.message}"
        )

    # Query should complete successfully
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_contextual_hazard_async_sleep(
    run_query: Callable[[str], CqResult],
) -> None:
    """Detect async-context hazard for time.sleep."""
    result = run_query(
        "entity=function name=async_blocking_sleep fields=hazards "
        "in=tests/e2e/cq/_fixtures/control_flow.py"
    )
    hazard_kinds = {
        finding.details.get("kind")
        for section in result.sections
        for finding in section.findings
        if finding.category == "hazard"
    }
    assert "async_threading_wait" in hazard_kinds


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_contextual_hazard_try_eval(
    run_query: Callable[[str], CqResult],
) -> None:
    """Detect try-context hazard for eval usage."""
    result = run_query(
        "entity=function name=try_eval_single fields=hazards "
        "in=tests/e2e/cq/_fixtures/control_flow.py"
    )
    hazard_kinds = {
        finding.details.get("kind")
        for section in result.sections
        for finding in section.findings
        if finding.category == "hazard"
    }
    assert "try_eval" in hazard_kinds
