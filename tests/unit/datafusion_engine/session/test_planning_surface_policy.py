"""Tests for typed planning surface contract compilation."""

from __future__ import annotations

from datafusion_engine.session.planning_surface_contract import (
    PLANNING_SURFACE_POLICY_CONTRACT_VERSION,
    PlanningSurfacePolicyContractV1,
    planning_surface_policy_contract_from_bundle,
)
from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig


def test_planning_surface_policy_defaults_from_bundle() -> None:
    """Bundle defaults compile to enabled relation/type planning policy."""
    policy = planning_surface_policy_contract_from_bundle(PolicyBundleConfig())
    assert isinstance(policy, PlanningSurfacePolicyContractV1)
    assert policy.enable_default_features is True
    assert policy.expr_planner_names == ("codeanatomy_domain",)
    assert policy.relation_planner_enabled is True
    assert policy.type_planner_enabled is True
    assert policy.table_factory_allowlist == ("delta",)


def test_planning_surface_policy_payload_is_deterministic() -> None:
    """Payload conversion is stable and preserves explicit values."""
    policy = PlanningSurfacePolicyContractV1(
        enable_default_features=True,
        expr_planner_names=("a", "b"),
        relation_planner_enabled=True,
        type_planner_enabled=False,
        table_factory_allowlist=("delta",),
    )
    assert policy.payload() == {
        "version": PLANNING_SURFACE_POLICY_CONTRACT_VERSION,
        "enable_default_features": True,
        "expr_planner_names": ["a", "b"],
        "relation_planner_enabled": True,
        "type_planner_enabled": False,
        "table_factory_allowlist": ["delta"],
    }
