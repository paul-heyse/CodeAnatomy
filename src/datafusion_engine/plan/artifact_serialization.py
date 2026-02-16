"""Serialization helpers for plan artifacts."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


def bundle_payload(bundle: DataFusionPlanArtifact) -> Mapping[str, object]:
    """Return canonical payload representation for a plan bundle."""
    return {
        "plan_identity_hash": bundle.plan_identity_hash,
        "plan_fingerprint": bundle.plan_fingerprint,
        "substrait_bytes": bundle.substrait_bytes,
        "has_execution_plan": bundle.execution_plan is not None,
    }


__all__ = ["bundle_payload"]
