"""Validation helpers for plan artifacts."""

from __future__ import annotations

from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


def validate_plan_identity(bundle: DataFusionPlanArtifact) -> None:
    """Validate that key identity fields are populated.

    Raises:
        ValueError: If required identity fields are missing.
    """
    if not bundle.plan_identity_hash:
        msg = "Plan artifact requires plan_identity_hash."
        raise ValueError(msg)
    if not bundle.plan_fingerprint:
        msg = "Plan artifact requires plan_fingerprint."
        raise ValueError(msg)


__all__ = ["validate_plan_identity"]
