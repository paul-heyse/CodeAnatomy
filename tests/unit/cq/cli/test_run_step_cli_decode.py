"""Tests for CLI run-step to canonical run-step conversion."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.contracts import to_run_step
from tools.cq.run.spec import QStep


def test_to_run_step_decodes_q_step() -> None:
    step = to_run_step({"type": "q", "query": "entity=function name=foo"})
    assert isinstance(step, QStep)


def test_to_run_step_raises_on_invalid_payload() -> None:
    with pytest.raises(ValueError, match="Invalid run step payload"):
        to_run_step({"type": "q"})
