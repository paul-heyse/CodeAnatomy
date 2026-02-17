"""Typed SearchStep mode tests."""

from __future__ import annotations

from tools.cq.run.spec import SearchStep
from tools.cq.run.step_decode import parse_run_step_json
from tools.cq.search._shared.types import QueryMode


def test_search_step_mode_accepts_query_mode() -> None:
    """SearchStep should store QueryMode values directly."""
    step = SearchStep(query="foo", mode=QueryMode.LITERAL)
    assert step.mode is QueryMode.LITERAL


def test_search_step_mode_decodes_from_json_as_query_mode() -> None:
    """Run-step decode should type mode as QueryMode at boundary."""
    step = parse_run_step_json('{"type":"search","query":"foo","mode":"regex"}')
    assert isinstance(step, SearchStep)
    assert step.mode is QueryMode.REGEX
