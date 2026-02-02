"""E2E tests for multi-rule inline ast-grep execution."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.query.executor import _execute_ast_grep_rules
from tools.cq.query.planner import AstGrepRule


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
def test_execute_ast_grep_rules_runs_multiple_rules(tmp_path: Path) -> None:
    """Inline ast-grep should return matches from all rules in one scan."""
    source = """
class Bar:
    def baz(self) -> int:
        return 1

def foo(x: int) -> int:
    return x + 1
"""
    file_path = tmp_path / "sample.py"
    file_path.write_text(source, encoding="utf-8")

    rules = (
        AstGrepRule(pattern="def foo($$$) -> $T: $$$"),
        AstGrepRule(pattern="class Bar: $$$"),
    )

    findings, records, _ = _execute_ast_grep_rules(
        rules,
        paths=[file_path],
        root=tmp_path,
    )

    rule_ids = {finding.details.get("rule_id") for finding in findings}
    assert "pattern_0" in rule_ids
    assert "pattern_1" in rule_ids
    assert len(records) >= 2
