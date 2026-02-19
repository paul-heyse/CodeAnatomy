"""Typed planning-surface policy contract."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig
from serde_msgspec import StructBaseStrict


class PlanningSurfacePolicyV1(StructBaseStrict, frozen=True):
    """Canonical typed planning policy for runtime/planning parity."""

    enable_default_features: bool = True
    expr_planner_names: tuple[str, ...] = ()
    relation_planner_enabled: bool = False
    type_planner_enabled: bool = False

    def payload(self) -> Mapping[str, object]:
        """Return deterministic mapping payload for cross-layer policy parity."""
        return {
            "enable_default_features": self.enable_default_features,
            "expr_planner_names": list(self.expr_planner_names),
            "relation_planner_enabled": self.relation_planner_enabled,
            "type_planner_enabled": self.type_planner_enabled,
        }


def planning_surface_policy_from_bundle(
    policies: PolicyBundleConfig,
) -> PlanningSurfacePolicyV1:
    """Compile a typed planning policy from policy-bundle configuration.

    Returns:
        PlanningSurfacePolicyV1: Typed planning policy derived from bundle
            hooks/planner names.
    """
    relation_enabled = bool(policies.expr_planner_hook) or bool(policies.expr_planner_names)
    type_enabled = relation_enabled or policies.type_planner_hook is not None
    return PlanningSurfacePolicyV1(
        enable_default_features=True,
        expr_planner_names=tuple(str(name) for name in policies.expr_planner_names),
        relation_planner_enabled=relation_enabled,
        type_planner_enabled=type_enabled,
    )


__all__ = [
    "PlanningSurfacePolicyV1",
    "planning_surface_policy_from_bundle",
]
