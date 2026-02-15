"""Tests for cq chain compilation."""

from __future__ import annotations

import pytest
from tools.cq.run.chain import compile_chain_segments
from tools.cq.run.spec import CallsStep, QStep, RunPlan


def test_chain_compiles_steps() -> None:
    """Ensure chain segments compile into a RunPlan."""
    plan = compile_chain_segments(
        [
            ["q", "entity=function name=foo"],
            ["calls", "foo"],
        ],
    )
    assert isinstance(plan, RunPlan)
    assert len(plan.steps) == 2
    assert isinstance(plan.steps[0], QStep)
    assert isinstance(plan.steps[1], CallsStep)


def test_chain_rejects_unused_tokens() -> None:
    """Ensure chain compilation surfaces unused forwarding tokens."""
    with pytest.raises(RuntimeError, match="Unused chain tokens"):
        compile_chain_segments([["calls", "foo", "--unknown"]])
