"""Persistence helpers for plan artifacts."""

from __future__ import annotations

from typing import Protocol

from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


class _PlanPersistenceStore(Protocol):
    def persist_plan(self, bundle: DataFusionPlanArtifact) -> object:
        """Persist a plan artifact bundle."""
        ...


def persist_bundle(store: _PlanPersistenceStore, bundle: DataFusionPlanArtifact) -> str:
    """Persist a plan bundle and return its identity hash.

    Returns:
        Plan identity hash for the persisted row.

    Raises:
        TypeError: If the persisted row does not expose a string
            ``plan_identity_hash``.
    """
    row = store.persist_plan(bundle)
    plan_identity_hash = getattr(row, "plan_identity_hash", None)
    if not isinstance(plan_identity_hash, str):
        msg = "Persisted plan row must expose string plan_identity_hash."
        raise TypeError(msg)
    return plan_identity_hash


__all__ = ["persist_bundle"]
