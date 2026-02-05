"""Tests for cq chain compilation."""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.context import CliContext
from tools.cq.run.chain import compile_chain_segments
from tools.cq.run.spec import CallsStep, QStep, RunPlan


def test_chain_compiles_steps(tmp_path: Path) -> None:
    """Ensure chain segments compile into a RunPlan."""
    ctx = CliContext.build(argv=["cq", "chain"], root=tmp_path)
    plan = compile_chain_segments(
        [
            ["q", "entity=function name=foo"],
            ["calls", "foo"],
        ],
        ctx,
    )
    assert isinstance(plan, RunPlan)
    assert len(plan.steps) == 2
    assert isinstance(plan.steps[0], QStep)
    assert isinstance(plan.steps[1], CallsStep)
