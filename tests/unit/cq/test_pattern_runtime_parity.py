"""Pattern runtime parity tests for findings and span filtering."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import (
    ExecutePlanRequestV1,
    _collect_match_spans,
    execute_plan,
)
from tools.cq.query.ir import Query
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import ToolPlan, compile_query, scope_to_globs, scope_to_paths

EXPECTED_VARIADIC_CAPTURE_NODES = 2


def _execute_query(tmp_path: Path, query_text: str) -> tuple[Query, ToolPlan, CqResult]:
    query = parse_query(query_text)
    plan = compile_query(query)
    tc = Toolchain.detect()
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(tmp_path),
            argv=("cq", "q", query_text),
            query_text=query_text,
        ),
        tc=tc,
    )
    return query, plan, result


def test_pattern_runtime_findings_and_spans_share_filter_semantics(tmp_path: Path) -> None:
    """Test pattern runtime findings and spans share filter semantics."""
    (tmp_path / "a.py").write_text("print(1)\nprint(2)\n", encoding="utf-8")

    query, plan, result = _execute_query(
        tmp_path,
        "pattern='print($A)' $A=~'^1$' lang=python",
    )

    anchors = [finding.anchor for finding in result.key_findings if finding.anchor is not None]
    assert len(anchors) == 1
    assert anchors[0].file == "a.py"
    assert anchors[0].line == 1

    paths = scope_to_paths(plan.scope, tmp_path.resolve())
    spans = _collect_match_spans(
        plan.sg_rules,
        paths,
        tmp_path.resolve(),
        query,
        scope_to_globs(plan.scope),
    )
    assert spans == {"a.py": [(1, 1)]}


def test_pattern_runtime_emits_variadic_metavar_captures(tmp_path: Path) -> None:
    """Test pattern runtime emits variadic metavar captures."""
    (tmp_path / "a.py").write_text("print(1, 2)\n", encoding="utf-8")

    _query, _plan, result = _execute_query(
        tmp_path,
        "pattern='print($$$ARGS)' lang=python",
    )

    finding = next(f for f in result.key_findings if f.category == "pattern_match")
    captures = finding.details.get("metavar_captures")
    assert isinstance(captures, dict)
    multi = captures.get("$$$ARGS")
    assert isinstance(multi, dict)
    assert multi.get("kind") == "multi"
    nodes = multi.get("nodes")
    assert isinstance(nodes, list)
    assert len(nodes) == EXPECTED_VARIADIC_CAPTURE_NODES


def test_pattern_runtime_executes_inline_rule_for_non_default_strictness(tmp_path: Path) -> None:
    """Test pattern runtime executes inline rule for non default strictness."""
    (tmp_path / "a.py").write_text("print(1)\n", encoding="utf-8")

    _query, _plan, result = _execute_query(
        tmp_path,
        "pattern='print($A)' strictness=ast lang=python",
    )

    anchors = [finding.anchor for finding in result.key_findings if finding.anchor is not None]
    assert anchors
    assert anchors[0].file == "a.py"
    assert anchors[0].line == 1
