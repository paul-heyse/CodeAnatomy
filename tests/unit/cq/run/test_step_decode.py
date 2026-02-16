"""Tests for run step JSON decoding."""

from __future__ import annotations

import pytest
from tools.cq.core.typed_boundary import BoundaryDecodeError
from tools.cq.run.spec import CallsStep, QStep, SearchStep
from tools.cq.run.step_decode import parse_run_step_json, parse_run_steps_json

MULTI_STEP_COUNT = 3


def test_parse_run_step_json_q_step() -> None:
    """Test parsing a single q step from JSON."""
    raw = '{"type": "q", "query": "entity=function"}'
    step = parse_run_step_json(raw)
    assert isinstance(step, QStep)
    assert step.query == "entity=function"
    assert step.id is None


def test_parse_run_step_json_search_step() -> None:
    """Test parsing a single search step from JSON."""
    raw = '{"type": "search", "query": "build_graph", "mode": "regex"}'
    step = parse_run_step_json(raw)
    assert isinstance(step, SearchStep)
    assert step.query == "build_graph"
    assert step.mode == "regex"


def test_parse_run_step_json_calls_step() -> None:
    """Test parsing a single calls step from JSON."""
    raw = '{"type": "calls", "function": "build_graph", "id": "step_1"}'
    step = parse_run_step_json(raw)
    assert isinstance(step, CallsStep)
    assert step.function == "build_graph"
    assert step.id == "step_1"


def test_parse_run_step_json_invalid() -> None:
    """Test parsing invalid JSON raises BoundaryDecodeError."""
    with pytest.raises(BoundaryDecodeError, match="Invalid run step JSON"):
        parse_run_step_json('{"type": "unknown"}')


def test_parse_run_step_json_malformed() -> None:
    """Test parsing malformed JSON raises BoundaryDecodeError."""
    with pytest.raises(BoundaryDecodeError, match="Invalid run step JSON"):
        parse_run_step_json("{not valid json")


def test_parse_run_steps_json_empty() -> None:
    """Test parsing an empty array."""
    raw = "[]"
    steps = parse_run_steps_json(raw)
    assert steps == []


def test_parse_run_steps_json_multiple() -> None:
    """Test parsing multiple steps from JSON array."""
    raw = """
    [
        {"type": "q", "query": "entity=function"},
        {"type": "search", "query": "build_graph"},
        {"type": "calls", "function": "foo"}
    ]
    """
    steps = parse_run_steps_json(raw)
    assert len(steps) == MULTI_STEP_COUNT
    assert isinstance(steps[0], QStep)
    assert isinstance(steps[1], SearchStep)
    assert isinstance(steps[2], CallsStep)


def test_parse_run_steps_json_invalid_item() -> None:
    """Test parsing array with invalid item raises BoundaryDecodeError."""
    raw = '[{"type": "q", "query": "foo"}, {"type": "invalid"}]'
    with pytest.raises(BoundaryDecodeError, match="Invalid run steps JSON array"):
        parse_run_steps_json(raw)


def test_parse_run_steps_json_malformed() -> None:
    """Test parsing malformed JSON array raises BoundaryDecodeError."""
    with pytest.raises(BoundaryDecodeError, match="Invalid run steps JSON array"):
        parse_run_steps_json("[not valid json")
