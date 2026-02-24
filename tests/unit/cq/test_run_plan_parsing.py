"""Tests for cq run plan parsing."""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.app import app
from tools.cq.cli_app.options import RunOptions, options_from_params
from tools.cq.run.loader import load_run_plan
from tools.cq.run.spec import CallsStep, NeighborhoodStep, QStep, RunLoadInput, SearchStep

MULTI_STEP_PLAN_COUNT = 2
DEFAULT_NEIGHBORHOOD_TOP_K = 10
CUSTOM_NEIGHBORHOOD_TOP_K = 3


def _to_load_input(options: RunOptions) -> RunLoadInput:
    return RunLoadInput(
        plan=options.plan,
        step=tuple(options.step),
        steps=tuple(options.steps),
    )


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
    plan = load_run_plan(_to_load_input(options))
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
    plan = load_run_plan(_to_load_input(options))
    assert len(plan.steps) == MULTI_STEP_PLAN_COUNT
    assert isinstance(plan.steps[0], QStep)
    assert isinstance(plan.steps[1], CallsStep)


def test_run_plan_inline_neighborhood_step() -> None:
    """Ensure --step neighborhood JSON parses into a RunPlan."""
    _cmd, bound, _ignored = app.parse_args(
        [
            "run",
            "--step",
            '{"type":"neighborhood","target":"tools/cq/search/python_analysis_session.py:1"}',
        ],
        exit_on_error=False,
        print_error=False,
    )
    opts = bound.kwargs["opts"]
    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(_to_load_input(options))
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], NeighborhoodStep)
    assert plan.steps[0].target == "tools/cq/search/python_analysis_session.py:1"
    assert plan.steps[0].lang == "python"
    assert plan.steps[0].top_k == DEFAULT_NEIGHBORHOOD_TOP_K
    assert plan.steps[0].semantic_enrichment is True


def test_run_plan_inline_neighborhood_steps_array() -> None:
    """Ensure --steps neighborhood JSON array parses into a RunPlan."""
    _cmd, bound, _ignored = app.parse_args(
        [
            "run",
            "--steps",
            (
                '[{"type":"neighborhood","target":"tools/cq/search/python_analysis_session.py:1",'
                '"lang":"python","top_k":3,"semantic_enrichment":false}]'
            ),
        ],
        exit_on_error=False,
        print_error=False,
    )
    opts = bound.kwargs["opts"]
    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(_to_load_input(options))
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], NeighborhoodStep)
    assert plan.steps[0].target == "tools/cq/search/python_analysis_session.py:1"
    assert plan.steps[0].lang == "python"
    assert plan.steps[0].top_k == CUSTOM_NEIGHBORHOOD_TOP_K
    assert plan.steps[0].semantic_enrichment is False


def test_run_plan_inline_mixed_steps_array_with_neighborhood() -> None:
    """Ensure neighborhood payloads coexist with other step types in --steps arrays."""
    _cmd, bound, _ignored = app.parse_args(
        [
            "run",
            "--steps",
            (
                '[{"type":"q","query":"entity=function name=foo"},'
                '{"type":"neighborhood","target":"tools/cq/search/python_analysis_session.py:1"}]'
            ),
        ],
        exit_on_error=False,
        print_error=False,
    )
    opts = bound.kwargs["opts"]
    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(_to_load_input(options))
    assert len(plan.steps) == MULTI_STEP_PLAN_COUNT
    assert isinstance(plan.steps[0], QStep)
    assert isinstance(plan.steps[1], NeighborhoodStep)


def test_run_plan_inline_search_alias_fields() -> None:
    """Ensure --step search payload accepts lang/in aliases."""
    _cmd, bound, _ignored = app.parse_args(
        [
            "run",
            "--step",
            '{"type":"search","query":"register_udf","lang":"rust","in":"rust"}',
        ],
        exit_on_error=False,
        print_error=False,
    )
    opts = bound.kwargs["opts"]
    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(_to_load_input(options))
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], SearchStep)
    assert plan.steps[0].lang_scope == "rust"
    assert plan.steps[0].in_dir == "rust"


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
    plan = load_run_plan(_to_load_input(options))
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], QStep)
