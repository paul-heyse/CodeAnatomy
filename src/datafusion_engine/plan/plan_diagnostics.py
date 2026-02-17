"""Plan diagnostics helpers extracted from bundle_artifact."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import DataFrame

from datafusion_engine.plan.bundle_artifact import PlanDetailInputs, _plan_details


def collect_plan_details(
    df: DataFrame,
    *,
    detail_inputs: PlanDetailInputs,
) -> Mapping[str, object]:
    """Collect plan-detail diagnostics payload from prepared inputs.

    Returns:
        Mapping[str, object]: Diagnostic payload for plan details.
    """
    payload = _plan_details(df, detail_inputs=detail_inputs)
    return dict(payload)


__all__ = ["collect_plan_details"]
