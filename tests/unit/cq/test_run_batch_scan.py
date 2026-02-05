"""Tests for batched q step execution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep import sgpy_scanner
from tools.cq.cli_app.context import CliContext
from tools.cq.run.runner import execute_run_plan
from tools.cq.run.spec import QStep, RunPlan


def test_batch_q_steps_scan_once(monkeypatch, tmp_path: Path) -> None:
    """Ensure multiple q steps share a single ast-grep scan."""
    (tmp_path / "a.py").write_text("def foo():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("class Bar:\n    pass\n", encoding="utf-8")

    calls = 0
    real_scan = sgpy_scanner.scan_files

    def wrapped(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_scan(*args, **kwargs)

    monkeypatch.setattr(sgpy_scanner, "scan_files", wrapped)

    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    plan = RunPlan(
        steps=(
            QStep(query="entity=function name=foo"),
            QStep(query="entity=class name=Bar"),
        )
    )

    execute_run_plan(plan, ctx)
    assert calls == 1
