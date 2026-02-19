"""Versioned planning-surface policy contract shared across runtimes."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig
from serde_msgspec import StructBaseStrict
from utils.hashing import hash_json_default

PLANNING_SURFACE_POLICY_CONTRACT_VERSION = "v1"


class PlanningSurfacePolicyContractV1(StructBaseStrict, frozen=True):
    """Typed planning policy contract for Python/Rust parity."""

    enable_default_features: bool = True
    expr_planner_names: tuple[str, ...] = ()
    relation_planner_enabled: bool = False
    type_planner_enabled: bool = False
    table_factory_allowlist: tuple[str, ...] = ()

    def payload(self) -> Mapping[str, object]:
        """Return canonical JSON-safe payload for manifest parity checks."""
        return {
            "version": PLANNING_SURFACE_POLICY_CONTRACT_VERSION,
            "enable_default_features": self.enable_default_features,
            "expr_planner_names": list(self.expr_planner_names),
            "relation_planner_enabled": self.relation_planner_enabled,
            "type_planner_enabled": self.type_planner_enabled,
            "table_factory_allowlist": list(self.table_factory_allowlist),
        }


def planning_surface_policy_contract_from_bundle(
    policies: PolicyBundleConfig,
) -> PlanningSurfacePolicyContractV1:
    """Build typed planning policy contract from runtime policy bundle.

    Returns:
        PlanningSurfacePolicyContractV1: Normalized planning-surface contract payload.
    """
    relation_enabled = bool(policies.expr_planner_hook) or bool(policies.expr_planner_names)
    type_enabled = relation_enabled or policies.type_planner_hook is not None
    return PlanningSurfacePolicyContractV1(
        enable_default_features=True,
        expr_planner_names=tuple(str(name) for name in policies.expr_planner_names),
        relation_planner_enabled=relation_enabled,
        type_planner_enabled=type_enabled,
        table_factory_allowlist=("delta",),
    )


def planning_surface_policy_hash(policy: PlanningSurfacePolicyContractV1) -> str:
    """Return deterministic policy hash for parity checks."""
    return hash_json_default(msgspec.to_builtins(policy), str_keys=True)


__all__ = [
    "PLANNING_SURFACE_POLICY_CONTRACT_VERSION",
    "PlanningSurfacePolicyContractV1",
    "planning_surface_policy_contract_from_bundle",
    "planning_surface_policy_hash",
]
