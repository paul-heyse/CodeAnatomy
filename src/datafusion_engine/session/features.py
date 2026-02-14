"""Feature gate resolution and capability detection.

Extracted from session/runtime.py to isolate feature-gate state
snapshot logic from session construction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core_types import DeterminismTier
from serde_msgspec import StructBaseCompat

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class FeatureStateSnapshot(
    StructBaseCompat,
    array_like=True,
    gc=False,
    cache_hash=True,
    frozen=True,
):
    """Snapshot of runtime feature gates and determinism tier."""

    profile_name: str
    determinism_tier: DeterminismTier
    dynamic_filters_enabled: bool
    spill_enabled: bool
    named_args_supported: bool

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for diagnostics sinks.

        Returns:
        -------
        dict[str, object]
            Row mapping for diagnostics table ingestion.
        """
        return {
            "profile_name": self.profile_name,
            "determinism_tier": self.determinism_tier.value,
            "dynamic_filters_enabled": self.dynamic_filters_enabled,
            "spill_enabled": self.spill_enabled,
            "named_args_supported": self.named_args_supported,
        }


def feature_state_snapshot(
    *,
    profile_name: str,
    determinism_tier: DeterminismTier,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> FeatureStateSnapshot:
    """Build a feature state snapshot for diagnostics.

    Parameters
    ----------
    profile_name
        Human-readable profile name.
    determinism_tier
        Determinism tier for the runtime profile.
    runtime_profile
        Optional runtime profile to inspect for feature flags.

    Returns:
    -------
    FeatureStateSnapshot
        Snapshot describing runtime feature state.
    """
    if runtime_profile is None:
        return FeatureStateSnapshot(
            profile_name=profile_name,
            determinism_tier=determinism_tier,
            dynamic_filters_enabled=False,
            spill_enabled=False,
            named_args_supported=False,
        )
    gates = runtime_profile.policies.feature_gates
    dynamic_filters_enabled = (
        gates.enable_dynamic_filter_pushdown
        and gates.enable_join_dynamic_filter_pushdown
        and gates.enable_aggregate_dynamic_filter_pushdown
        and gates.enable_topk_dynamic_filter_pushdown
    )
    spill_enabled = runtime_profile.execution.spill_dir is not None
    return FeatureStateSnapshot(
        profile_name=profile_name,
        determinism_tier=determinism_tier,
        dynamic_filters_enabled=dynamic_filters_enabled,
        spill_enabled=spill_enabled,
        named_args_supported=named_args_supported(runtime_profile),
    )


def named_args_supported(profile: DataFusionRuntimeProfile) -> bool:
    """Return whether named arguments are enabled for SQL execution.

    Parameters
    ----------
    profile
        Runtime profile to evaluate.

    Returns:
    -------
    bool
        ``True`` when named arguments should be supported.
    """
    if not profile.features.enable_expr_planners:
        return False
    if profile.policies.expr_planner_hook is not None:
        return True
    if not profile.policies.expr_planner_names:
        return False
    from datafusion_engine.udf.runtime import extension_capabilities_report

    try:
        report = extension_capabilities_report()
    except (RuntimeError, TypeError, ValueError):
        return False
    return bool(report.get("available")) and bool(report.get("compatible"))
