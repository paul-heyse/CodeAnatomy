"""Tests for batched q step execution."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.astgrep.sgpy_scanner import RuleSpec, SgRecord
from tools.cq.cli_app.context import CliContext
from tools.cq.core.schema import CqResult
from tools.cq.query import batch as batch_queries
from tools.cq.query.executor_runtime import ExecutePlanRequestV1, execute_plan
from tools.cq.query.language import QueryLanguage
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query
from tools.cq.run.runner import execute_run_plan
from tools.cq.run.spec import QStep, RunPlan, SearchStep


def test_batch_q_steps_scan_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Ensure multiple q steps share a single ast-grep scan."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("class Bar:\n    pass\n", encoding="utf-8")

    calls = 0
    real_scan = batch_queries.scan_files

    observed_prefilter: list[bool] = []

    def wrapped(
        files: list[Path],
        rules: tuple[RuleSpec, ...],
        root: Path,
        *,
        lang: QueryLanguage = "python",
        prefilter: bool = True,
    ) -> list[SgRecord]:
        nonlocal calls
        calls += 1
        observed_prefilter.append(prefilter)
        return real_scan(files, rules, root, lang=lang, prefilter=prefilter)

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
    assert observed_prefilter == [True]


def test_batch_equivalence_single_query(tmp_path: Path) -> None:
    """Ensure a single batched q step matches non-batch execution."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("def bar():\n    return 2\n", encoding="utf-8")

    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    query_string = "entity=function name=foo lang=python"
    parsed = parse_query(query_string)
    plan = compile_query(parsed)
    single = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=parsed,
            root=str(ctx.root),
            services=ctx.services,
            argv=tuple(ctx.argv),
        ),
        tc=ctx.toolchain,
    )

    batched = execute_run_plan(RunPlan(steps=(QStep(query=query_string),)), ctx)

    def normalize(result: CqResult) -> set[tuple[str, int, str]]:
        return {
            (finding.anchor.file, finding.anchor.line, finding.message)
            for finding in result.key_findings
            if finding.anchor is not None
        }

    assert normalize(single) == normalize(batched)


def test_batch_q_steps_auto_scope_collapses_per_parent_step(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Auto-scope q steps should batch per language and collapse to parent step ids."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("def bar():\n    return 2\n", encoding="utf-8")

    calls = 0
    real_scan = batch_queries.scan_files

    observed_prefilter: list[bool] = []

    def wrapped(
        files: list[Path],
        rules: tuple[RuleSpec, ...],
        root: Path,
        *,
        lang: QueryLanguage = "python",
        prefilter: bool = True,
    ) -> list[SgRecord]:
        nonlocal calls
        calls += 1
        observed_prefilter.append(prefilter)
        return real_scan(files, rules, root, lang=lang, prefilter=prefilter)

    monkeypatch.setattr(batch_queries, "scan_files", wrapped)

    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    plan = RunPlan(
        steps=(
            QStep(id="q_auto_0", query="entity=function name=foo lang=auto"),
            QStep(id="q_auto_1", query="entity=function name=bar lang=auto"),
        )
    )
    result = execute_run_plan(plan, ctx)

    # Python files only => one shared scan for python partition.
    assert calls == 1
    assert observed_prefilter == [True]
    steps = result.summary.get("steps")
    assert isinstance(steps, list)
    assert steps.count("q_auto_0") == 1
    assert steps.count("q_auto_1") == 1


def test_run_top_level_summary_uses_single_q_metadata(tmp_path: Path) -> None:
    """Single q-step run should expose non-empty top-level query/mode metadata."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    result = execute_run_plan(
        RunPlan(steps=(QStep(query="entity=function name=foo lang=python"),)),
        ctx,
    )

    assert result.summary.get("mode") == "entity"
    assert result.summary.get("query") == "entity=function name=foo lang=python"
    assert result.summary.get("lang_scope") == "python"
    assert result.summary.get("language_order") == ["python"]


def test_run_top_level_summary_synthesizes_for_mixed_steps(tmp_path: Path) -> None:
    """Mixed run plans should expose synthetic top-level query/mode metadata."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    result = execute_run_plan(
        RunPlan(
            steps=(
                QStep(query="entity=function name=foo lang=python"),
                SearchStep(query="foo"),
            )
        ),
        ctx,
    )

    assert result.summary.get("mode") == "run"
    assert result.summary.get("query") == "multi-step plan (2 steps)"
    assert result.summary.get("lang_scope") == "auto"
    assert result.summary.get("language_order") == ["python", "rust"]
