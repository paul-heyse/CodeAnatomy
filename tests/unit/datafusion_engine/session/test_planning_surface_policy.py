"""Tests for typed planning surface policy compilation."""

from __future__ import annotations

from datafusion_engine.session.planning_surface_policy import (
    PlanningSurfacePolicyV1,
    planning_surface_policy_from_bundle,
)
from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig


def test_planning_surface_policy_defaults_from_bundle() -> None:
    """Bundle defaults compile to enabled relation/type planning policy."""
    policy = planning_surface_policy_from_bundle(PolicyBundleConfig())
    assert isinstance(policy, PlanningSurfacePolicyV1)
    assert policy.enable_default_features is True
    assert policy.expr_planner_names == ("codeanatomy_domain",)
    assert policy.relation_planner_enabled is True
    assert policy.type_planner_enabled is True


def test_planning_surface_policy_payload_is_deterministic() -> None:
    """Payload conversion is stable and preserves explicit values."""
    policy = PlanningSurfacePolicyV1(
        enable_default_features=True,
        expr_planner_names=("a", "b"),
        relation_planner_enabled=True,
        type_planner_enabled=False,
    )
    assert policy.payload() == {
        "enable_default_features": True,
        "expr_planner_names": ["a", "b"],
        "relation_planner_enabled": True,
        "type_planner_enabled": False,
    }
