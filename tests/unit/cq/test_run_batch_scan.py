"""Tests for batched q step execution."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.astgrep.sgpy_scanner import RuleSpec, SgRecord
from tools.cq.cli_app.context import CliContext
from tools.cq.core.schema import CqResult
from tools.cq.query import batch as batch_queries
from tools.cq.query.executor import execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.run.runner import execute_run_plan
from tools.cq.run.spec import QStep, RunPlan


def test_batch_q_steps_scan_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Ensure multiple q steps share a single ast-grep scan."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("class Bar:\n    pass\n", encoding="utf-8")

    calls = 0
    real_scan = batch_queries.scan_files

    def wrapped(files: list[Path], rules: tuple[RuleSpec, ...], root: Path) -> list[SgRecord]:
        nonlocal calls
        calls += 1
        return real_scan(files, rules, root)

    monkeypatch.setattr(batch_queries, "scan_files", wrapped)

    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    plan = RunPlan(
        steps=(
            QStep(query="entity=function name=foo lang=python"),
            QStep(query="entity=class name=Bar lang=python"),
        )
    )

    execute_run_plan(plan, ctx)
    assert calls == 1


def test_batch_equivalence_single_query(tmp_path: Path) -> None:
    """Ensure a single batched q step matches non-batch execution."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("def bar():\n    return 2\n", encoding="utf-8")

    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    query_string = "entity=function name=foo lang=python"
    parsed = parse_query(query_string)
    plan = compile_query(parsed)
    single = execute_plan(plan=plan, query=parsed, tc=ctx.toolchain, root=ctx.root, argv=ctx.argv)

    batched = execute_run_plan(RunPlan(steps=(QStep(query=query_string),)), ctx)

    def normalize(result: CqResult) -> set[tuple[str, int, str]]:
        return {
            (finding.anchor.file, finding.anchor.line, finding.message)
            for finding in result.key_findings
            if finding.anchor is not None
        }

    assert normalize(single) == normalize(batched)
