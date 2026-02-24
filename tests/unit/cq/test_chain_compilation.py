"""Tests for cq chain compilation."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.app import app
from tools.cq.run.chain import compile_chain_segments
from tools.cq.run.spec import CallsStep, QStep, RunPlan, SearchStep

CHAIN_STEP_COUNT = 2


def test_chain_compiles_steps() -> None:
    """Ensure chain segments compile into a RunPlan."""
    plan = compile_chain_segments(
        [
            ["q", "entity=function name=foo"],
            ["calls", "foo"],
        ],
        cli_app=app,
    )
    assert isinstance(plan, RunPlan)
    assert len(plan.steps) == CHAIN_STEP_COUNT
    assert isinstance(plan.steps[0], QStep)
    assert isinstance(plan.steps[1], CallsStep)


def test_chain_rejects_unused_tokens() -> None:
    """Ensure chain compilation surfaces unused forwarding tokens."""
    with pytest.raises(RuntimeError, match="Unused chain tokens"):
        compile_chain_segments([["calls", "foo", "--unknown"]], cli_app=app)


def test_chain_search_forwards_lang_scope() -> None:
    """Chain search segments should propagate --lang into SearchStep.lang_scope."""
    plan = compile_chain_segments(
        [
            ["search", "register_udf", "--lang", "rust", "--in", "rust"],
        ],
        cli_app=app,
    )
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], SearchStep)
    assert plan.steps[0].lang_scope == "rust"
    assert plan.steps[0].in_dir == "rust"
