"""E2E tests for method name collision handling."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_method_collision_self_receiver_filter(
    run_query: Callable[[str], CqResult],
) -> None:
    """Self/cls receiver calls should respect class-local method targets."""
    result = run_query(
        "entity=function name=collide in=tests/e2e/cq/_fixtures/method_collision.py fields=callers"
    )

    assert len(result.key_findings) == 1
    caller_findings = [
        finding
        for section in result.sections
        for finding in section.findings
        if finding.category == "caller"
    ]
    assert len(caller_findings) == 0
