"""Tests for CLI run-step to canonical run-step conversion."""

from __future__ import annotations

import pytest
from tools.cq.core.typed_boundary import BoundaryDecodeError
from tools.cq.run.spec import QStep
from tools.cq.run.step_decode import parse_run_step_json


def test_parse_run_step_json_decodes_q_step() -> None:
    """Test decoding a q step from JSON."""
    step = parse_run_step_json('{"type": "q", "query": "entity=function name=foo"}')
    assert isinstance(step, QStep)
    assert step.query == "entity=function name=foo"


def test_parse_run_step_json_raises_on_invalid_payload() -> None:
    """Test decoding raises on invalid payload."""
    with pytest.raises(BoundaryDecodeError, match="Invalid run step JSON"):
        parse_run_step_json('{"type": "q"}')
