"""Tests for run-step enrichment flags."""

from __future__ import annotations

from tools.cq.run.spec import NeighborhoodStep, SearchStep
from tools.cq.run.step_decode import parse_run_step_json


def test_parse_search_step_enrichment_flags() -> None:
    """Parse search run step with enrichment flags."""
    step = parse_run_step_json(
        '{"type":"search","query":"foo","enrich":false,"enrich_mode":"full"}'
    )
    assert isinstance(step, SearchStep)
    assert step.enrich is False
    assert step.enrich_mode == "full"


def test_parse_neighborhood_step_enrichment_flags() -> None:
    """Parse neighborhood run step with enrichment flags."""
    step = parse_run_step_json(
        '{"type":"neighborhood","target":"foo","enrich":true,"enrich_mode":"ts_sym_dis"}'
    )
    assert isinstance(step, NeighborhoodStep)
    assert step.enrich is True
    assert step.enrich_mode == "ts_sym_dis"
