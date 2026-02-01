"""Tests for materialization cache decision logic."""

from __future__ import annotations

from datafusion_engine.materialize_policy import MaterializationPolicy
from engine.materialize_pipeline import resolve_materialization_cache_decision


def test_semantic_cache_policy_overrides_engine_policy() -> None:
    """Semantic cache policy takes precedence over engine heuristics."""
    policy = MaterializationPolicy()
    decision = resolve_materialization_cache_decision(
        policy=policy,
        prefer_reader=True,
        params=None,
        semantic_cache_policy="delta_output",
    )
    assert decision.enabled is True
    assert decision.reason == "semantic_policy_delta_output"


def test_semantic_cache_policy_none_disables_cache() -> None:
    """Semantic cache policy of 'none' disables caching."""
    policy = MaterializationPolicy()
    decision = resolve_materialization_cache_decision(
        policy=policy,
        prefer_reader=False,
        params=None,
        semantic_cache_policy="none",
    )
    assert decision.enabled is False
    assert decision.reason == "semantic_policy_none"
