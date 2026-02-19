"""Planning-surface contract tests for session policy payload stability."""

from __future__ import annotations

import msgspec

from datafusion_engine.session.planning_surface_contract import (
    PLANNING_SURFACE_POLICY_CONTRACT_VERSION,
    planning_surface_policy_contract_from_bundle,
    planning_surface_policy_hash,
)
from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig


def test_planning_surface_contract_payload_is_stable() -> None:
    """Contract payload and serialization should remain stable for fixed input."""
    policies = PolicyBundleConfig(expr_planner_names=("codeanatomy_domain",))
    contract = planning_surface_policy_contract_from_bundle(policies)

    payload = contract.payload()
    assert payload["version"] == PLANNING_SURFACE_POLICY_CONTRACT_VERSION
    assert payload["expr_planner_names"] == ["codeanatomy_domain"]
    assert payload["relation_planner_enabled"] is True

    serialized = msgspec.to_builtins(contract)
    assert serialized["expr_planner_names"] == ("codeanatomy_domain",)


def test_planning_surface_policy_hash_changes_with_policy_content() -> None:
    """Hash should change when planning-surface policy content changes."""
    a = planning_surface_policy_contract_from_bundle(
        PolicyBundleConfig(expr_planner_names=("codeanatomy_domain",))
    )
    b = planning_surface_policy_contract_from_bundle(PolicyBundleConfig(expr_planner_names=()))

    assert planning_surface_policy_hash(a) != planning_surface_policy_hash(b)
