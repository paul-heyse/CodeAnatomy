"""Engine runtime composition helpers."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from obs.diagnostics import DiagnosticsCollector
    from relspec.pipeline_policy import DiagnosticsPolicy

from obs.otel import configure_otel


@dataclass(frozen=True)
class EngineRuntime:
    """Unified runtime settings for engine execution surfaces."""

    datafusion_profile: DataFusionRuntimeProfile

    def with_datafusion_profile(
        self,
        profile: DataFusionRuntimeProfile,
    ) -> EngineRuntime:
        """Return a copy of the runtime with updated DataFusion profile.

        Returns:
        -------
        EngineRuntime
            Updated engine runtime bundle.
        """
        return dataclass_replace(
            self,
            datafusion_profile=profile,
        )


def build_engine_runtime(
    *,
    runtime_profile: DataFusionRuntimeProfile,
    diagnostics: DiagnosticsCollector | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
) -> EngineRuntime:
    """Build the unified runtime bundle for engine execution.

    Returns:
    -------
    EngineRuntime
        Bundled runtime settings for DataFusion execution.
    """
    configure_otel(service_name="codeanatomy")
    datafusion_profile = runtime_profile
    if diagnostics_policy is not None:
        datafusion_profile = _apply_diagnostics_policy(
            datafusion_profile,
            diagnostics_policy,
        )
    if diagnostics is not None:
        datafusion_profile = msgspec.structs.replace(
            datafusion_profile,
            diagnostics=msgspec.structs.replace(
                datafusion_profile.diagnostics,
                diagnostics_sink=diagnostics,
            ),
        )
    return EngineRuntime(
        datafusion_profile=datafusion_profile,
    )


def _apply_diagnostics_policy(
    profile: DataFusionRuntimeProfile,
    policy: DiagnosticsPolicy,
) -> DataFusionRuntimeProfile:
    """Return a runtime profile updated with diagnostics settings.

    Returns:
    -------
    DataFusionRuntimeProfile
        Updated runtime profile with diagnostics settings applied.
    """
    capture_explain = policy.capture_datafusion_explains
    enable_metrics = policy.capture_datafusion_metrics
    enable_tracing = policy.capture_datafusion_traces
    capture_plan_artifacts = capture_explain
    return msgspec.structs.replace(
        profile,
        features=msgspec.structs.replace(
            profile.features,
            enable_metrics=enable_metrics,
            enable_tracing=enable_tracing,
        ),
        diagnostics=msgspec.structs.replace(
            profile.diagnostics,
            capture_explain=capture_explain,
            explain_analyze=policy.explain_analyze,
            explain_analyze_level=policy.explain_analyze_level,
            explain_collector=profile.diagnostics.explain_collector if capture_explain else None,
            capture_plan_artifacts=capture_plan_artifacts,
            plan_collector=profile.diagnostics.plan_collector if capture_plan_artifacts else None,
            metrics_collector=profile.diagnostics.metrics_collector if enable_metrics else None,
            tracing_collector=profile.diagnostics.tracing_collector if enable_tracing else None,
            emit_semantic_quality_diagnostics=policy.emit_semantic_quality_diagnostics,
        ),
    )


__all__ = ["EngineRuntime", "build_engine_runtime"]
