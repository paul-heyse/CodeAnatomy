"""E2E tests for cq import queries.

Tests the query command's ability to find and analyze imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult, Finding

MIN_IMPORT_FINDINGS_ACROSS_CODEBASE = 10
MIN_FILES_WITH_IMPORTS = 5


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_imports_in_module(
    run_query: Callable[[str], CqResult],
) -> None:
    """Find imports in a specific module.

    Tests that the query engine can:
    1. Identify import statements
    2. Extract module names
    3. Return proper file/line anchors
    """
    # Query for imports in the graph module
    result = run_query("entity=import in=src/graph/")

    # Should find at least some imports
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    valid_categories = {"import", "from_import"}
    import_findings = [f for f in all_findings if f.category in valid_categories]
    assert len(import_findings) > 0, "Expected to find imports in src/graph/"

    # All findings should be in src/graph/
    for finding in import_findings:
        if finding.anchor is not None:
            assert "src/graph/" in finding.anchor.file, (
                f"Import at {finding.anchor.file} not in src/graph/ scope"
            )
            assert finding.anchor.line > 0, "Line number should be positive"


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_import_entity(
    run_query: Callable[[str], CqResult],
) -> None:
    """Query entity=import to find all import statements.

    Verifies that entity=import returns import-type findings.
    """
    # Query for imports in a small scope to keep results manageable
    result = run_query("entity=import in=src/graph/product_build.py")

    # Collect all findings
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find at least one import
    assert len(all_findings) > 0, "Expected at least one import in product_build.py"

    # Verify findings have import category
    valid_categories = {"import", "from_import"}
    for finding in all_findings:
        assert finding.category in valid_categories, (
            f"Expected import/from_import category, got {finding.category}"
        )


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_import_with_name_filter(
    run_query: Callable[[str], CqResult],
    assert_finding_exists: Callable[..., Finding],
) -> None:
    """Query for imports of a specific module by name.

    Tests that name filtering works with import queries.
    """
    # Query for pathlib imports (very common)
    result = run_query("entity=import name=pathlib")

    # Should find pathlib imports
    pathlib_finding = assert_finding_exists(
        result,
        message_contains="pathlib",
    )

    assert pathlib_finding.anchor is not None
    assert pathlib_finding.anchor.line > 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_imports_in_nonexistent_scope(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test import query in a scope that does not exist.

    Should return empty results without error.
    """
    result = run_query("entity=import in=nonexistent_directory/")

    # Collect all findings
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should have no findings
    assert len(all_findings) == 0, "Expected no findings in nonexistent scope"

    # Run should still be valid
    assert result.run.macro == "q"
    assert result.run.elapsed_ms >= 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_import_details_structure(
    run_query: Callable[[str], CqResult],
) -> None:
    """Verify import findings have expected detail fields.

    Import findings should include module name and potentially other metadata.
    """
    result = run_query("entity=import in=src/graph/product_build.py")

    # Collect all findings
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find at least one import
    assert len(all_findings) > 0, "Expected at least one import"

    # Check first import has expected structure
    first_import = all_findings[0]
    assert first_import.anchor is not None, "Import should have anchor"
    assert first_import.anchor.file.endswith(".py"), "Import should be in Python file"
    assert first_import.message, "Import should have message"
    valid_categories = {"import", "from_import"}
    assert first_import.category in valid_categories, "Should be import category"


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_imports_across_codebase(
    run_query: Callable[[str], CqResult],
) -> None:
    """Test import query across entire codebase.

    Verifies that broad queries work and return reasonable results.
    """
    # Query for all imports (no scope filter)
    # This is a broad query but should complete
    result = run_query("entity=import")

    # Collect all findings
    all_findings = list(result.key_findings + result.evidence)
    for section in result.sections:
        all_findings.extend(section.findings)

    # Should find many imports across the codebase
    assert len(all_findings) > MIN_IMPORT_FINDINGS_ACROSS_CODEBASE, (
        "Expected many imports across codebase"
    )

    # Verify diverse files are represented
    files = {f.anchor.file for f in all_findings if f.anchor is not None}
    assert len(files) > MIN_FILES_WITH_IMPORTS, "Expected imports from multiple files"

    # Run should complete successfully
    assert result.run.elapsed_ms >= 0
