"""Parity-hash tests for planning-surface contract determinism."""

from __future__ import annotations

from datafusion_engine.session.planning_surface_contract import (
    PlanningSurfacePolicyContractV1,
    planning_surface_policy_hash,
)


def test_policy_parity_hash_is_deterministic() -> None:
    """Identical policy payloads should always hash to the same value."""
    policy = PlanningSurfacePolicyContractV1(
        enable_default_features=True,
        expr_planner_names=("codeanatomy_domain",),
        relation_planner_enabled=True,
        type_planner_enabled=True,
        table_factory_allowlist=("delta",),
    )

    assert planning_surface_policy_hash(policy) == planning_surface_policy_hash(policy)
