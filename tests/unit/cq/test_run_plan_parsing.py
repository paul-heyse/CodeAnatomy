"""Tests for cq run plan parsing."""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.app import app
from tools.cq.cli_app.options import RunOptions, options_from_params
from tools.cq.run.loader import load_run_plan
from tools.cq.run.spec import CallsStep, QStep


def test_run_plan_inline_step() -> None:
    """Ensure --step JSON parses into a RunPlan."""
    _cmd, bound, _ignored = app.parse_args(
        [
            "run",
            "--step",
            '{"type":"q","query":"entity=function name=foo"}',
        ],
        exit_on_error=False,
        print_error=False,
    )
    opts = bound.kwargs["opts"]
    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(options)
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], QStep)
    assert plan.steps[0].query == "entity=function name=foo"


def test_run_plan_inline_steps_array() -> None:
    """Ensure --steps JSON array parses into a RunPlan."""
    _cmd, bound, _ignored = app.parse_args(
        [
            "run",
            "--steps",
            '[{"type":"q","query":"entity=function name=foo"},{"type":"calls","function":"foo"}]',
        ],
        exit_on_error=False,
        print_error=False,
    )
    opts = bound.kwargs["opts"]
    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(options)
    assert len(plan.steps) == 2
    assert isinstance(plan.steps[0], QStep)
    assert isinstance(plan.steps[1], CallsStep)


def test_run_plan_toml(tmp_path: Path) -> None:
    """Ensure TOML run plans load correctly."""
    plan_path = tmp_path / "plan.toml"
    plan_path.write_text(
        "\n".join(
            [
                "version = 1",
                "",
                "[[steps]]",
                'type = "q"',
                'query = "entity=function name=foo"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    options = RunOptions(plan=plan_path)
    plan = load_run_plan(options)
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], QStep)
