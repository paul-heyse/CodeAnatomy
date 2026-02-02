"""E2E tests for scope filtering and relational constraints."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_entity_relational_constraint_filters_definitions(
    run_query: Callable[[str], CqResult],
) -> None:
    """Entity queries should honor relational constraints."""
    result = run_query(
        "entity=function inside='class ClosureFactory' in=tests/e2e/cq/_fixtures/closures.py"
    )
    names = {
        finding.details.get("name")
        for finding in result.key_findings
        if "name" in finding.details
    }
    assert "create_multiplier" in names
    assert "outer_function" not in names


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_scope_exclude_filters_results(
    run_query: Callable[[str], CqResult],
) -> None:
    """Exclude scope should remove matching files from results."""
    result = run_query(
        "entity=function name=outer_function in=tests/e2e/cq/_fixtures "
        "exclude=tests/e2e/cq/_fixtures/closures.py"
    )
    assert len(result.key_findings) == 0


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_scope_globs_limits_results(
    run_query: Callable[[str], CqResult],
) -> None:
    """Glob scope should restrict results to matching files."""
    result = run_query("entity=function globs=tests/e2e/cq/_fixtures/closures.py")
    assert len(result.key_findings) > 0
    for finding in result.key_findings:
        if finding.anchor is not None:
            assert finding.anchor.file == "tests/e2e/cq/_fixtures/closures.py"
