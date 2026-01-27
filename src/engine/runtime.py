"""Engine runtime composition helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from arrowdsl.core.execution_context import ExecutionContext

if TYPE_CHECKING:
    from arrowdsl.core.runtime_profiles import RuntimeProfile
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from obs.diagnostics import DiagnosticsCollector
    from relspec.pipeline_policy import DiagnosticsPolicy


@dataclass(frozen=True)
class EngineRuntime:
    """Unified runtime settings for engine execution surfaces."""

    runtime_profile: RuntimeProfile
    datafusion_profile: DataFusionRuntimeProfile | None

    def with_datafusion_profile(
        self,
        profile: DataFusionRuntimeProfile | None,
    ) -> EngineRuntime:
        """Return a copy of the runtime with updated DataFusion profile.

        Returns
        -------
        EngineRuntime
            Updated engine runtime bundle.
        """
        return replace(
            self,
            datafusion_profile=profile,
        )


def build_engine_runtime(
    *,
    ctx: ExecutionContext,
    runtime_profile: RuntimeProfile | None = None,
    diagnostics: DiagnosticsCollector | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
) -> EngineRuntime:
    """Build the unified runtime bundle for engine execution.

    Returns
    -------
    EngineRuntime
        Bundled runtime settings for DataFusion execution.
    """
    runtime = runtime_profile or ctx.runtime
    runtime.apply_global_thread_pools()
    datafusion_profile = runtime.datafusion
    if datafusion_profile is not None and diagnostics_policy is not None:
        datafusion_profile = _apply_diagnostics_policy(
            datafusion_profile,
            diagnostics_policy,
        )
    if datafusion_profile is not None and diagnostics is not None:
        datafusion_profile = replace(datafusion_profile, diagnostics_sink=diagnostics)
        runtime = runtime.with_datafusion(datafusion_profile)
    return EngineRuntime(
        runtime_profile=runtime,
        datafusion_profile=datafusion_profile,
    )


def _apply_diagnostics_policy(
    profile: DataFusionRuntimeProfile,
    policy: DiagnosticsPolicy,
) -> DataFusionRuntimeProfile:
    """Return a runtime profile updated with diagnostics settings.

    Returns
    -------
    DataFusionRuntimeProfile
        Updated runtime profile with diagnostics settings applied.
    """
    capture_explain = policy.capture_datafusion_explains
    enable_metrics = policy.capture_datafusion_metrics
    enable_tracing = policy.capture_datafusion_traces
    capture_plan_artifacts = capture_explain
    return replace(
        profile,
        capture_explain=capture_explain,
        explain_analyze=policy.explain_analyze,
        explain_analyze_level=policy.explain_analyze_level,
        explain_collector=profile.explain_collector if capture_explain else None,
        capture_plan_artifacts=capture_plan_artifacts,
        plan_collector=profile.plan_collector if capture_plan_artifacts else None,
        enable_metrics=enable_metrics,
        metrics_collector=profile.metrics_collector if enable_metrics else None,
        enable_tracing=enable_tracing,
        tracing_collector=profile.tracing_collector if enable_tracing else None,
    )


__all__ = ["EngineRuntime", "build_engine_runtime"]
