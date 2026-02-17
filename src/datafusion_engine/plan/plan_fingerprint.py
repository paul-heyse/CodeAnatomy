"""Plan fingerprint helpers extracted from bundle_artifact."""

from __future__ import annotations

from datafusion_engine.plan.bundle_artifact import PlanFingerprintInputs, _hash_plan


def compute_plan_fingerprint(inputs: PlanFingerprintInputs) -> str:
    """Compute deterministic fingerprint for a plan bundle.

    Returns:
        str: Stable fingerprint hash for the input bundle.
    """
    return _hash_plan(inputs)


__all__ = ["PlanFingerprintInputs", "compute_plan_fingerprint"]
