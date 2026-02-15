"""Tests for msgspec TOML run plan decoding."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.cli_app.options import RunOptions
from tools.cq.run.loader import RunPlanError, load_run_plan
from tools.cq.run.spec import QStep


def test_load_run_plan_decodes_toml_via_msgspec(tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.toml"
    plan_path.write_text(
        "\n".join(
            [
                "version = 1",
                "",
                "[[steps]]",
                'type = "q"',
                'query = "entity=function name=foo"',
            ]
        ),
        encoding="utf-8",
    )
    plan = load_run_plan(RunOptions(plan=plan_path))
    assert len(plan.steps) == 1
    assert isinstance(plan.steps[0], QStep)


def test_load_run_plan_raises_on_invalid_toml(tmp_path: Path) -> None:
    plan_path = tmp_path / "broken.toml"
    plan_path.write_text(
        "\n".join(
            [
                "version = 1",
                "[[steps]",
                'type = "q"',
                'query = "entity=function name=foo"',
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(RunPlanError, match="Invalid TOML in plan file"):
        load_run_plan(RunOptions(plan=plan_path))
