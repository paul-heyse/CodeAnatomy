"""Tests for pipeline contracts module (merged from partition_contracts + models)."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.contracts import (
    CandidateSearchRequest,
    SearchConfig,
    SearchPartitionPlanV1,
    SearchRequest,
)


def test_search_partition_plan_v1_round_trips() -> None:
    """Verify SearchPartitionPlanV1 constructs with required fields."""
    plan = SearchPartitionPlanV1(
        root="/repo",
        language="python",
        query="foo",
        mode="identifier",
    )
    assert plan.root == "/repo"
    assert plan.language == "python"
    assert plan.include_strings is False


def test_search_config_alias() -> None:
    """Verify `SearchConfig` contract is exposed from pipeline contracts."""
    assert SearchConfig.__name__ == "SearchConfig"


def test_search_request_defaults() -> None:
    """Verify SearchRequest has sensible defaults."""
    req = SearchRequest(root=Path("/repo"), query="bar")
    assert req.mode is None
    assert req.include_strings is False
    assert req.with_neighborhood is False


def test_candidate_search_request_required_fields() -> None:
    """Verify CandidateSearchRequest requires mode and limits."""
    from tools.cq.search.pipeline.profiles import SearchLimits

    limits = SearchLimits()
    req = CandidateSearchRequest(
        root=Path("/repo"),
        query="baz",
        mode=QueryMode.IDENTIFIER,
        limits=limits,
    )
    assert req.query == "baz"
