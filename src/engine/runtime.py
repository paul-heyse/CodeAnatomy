"""Engine runtime composition helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from arrowdsl.core.execution_context import ExecutionContext
from ibis_engine.config import IbisBackendConfig
from sqlglot_tools.optimizer import SqlGlotPolicy, default_sqlglot_policy

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
    ibis_config: IbisBackendConfig
    sqlglot_policy: SqlGlotPolicy

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
        ibis_config = replace(self.ibis_config, datafusion_profile=profile)
        return replace(
            self,
            datafusion_profile=profile,
            ibis_config=ibis_config,
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
        Bundled runtime settings for DataFusion, Ibis, and SQLGlot.
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
    ibis_config = IbisBackendConfig(
        datafusion_profile=datafusion_profile,
        fuse_selects=runtime.ibis_fuse_selects,
        default_limit=runtime.ibis_default_limit,
        default_dialect=runtime.ibis_default_dialect,
        interactive=runtime.ibis_interactive,
    )
    return EngineRuntime(
        runtime_profile=runtime,
        datafusion_profile=datafusion_profile,
        ibis_config=ibis_config,
        sqlglot_policy=default_sqlglot_policy(),
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
    return replace(
        profile,
        capture_explain=capture_explain,
        explain_analyze=policy.explain_analyze,
        explain_analyze_level=policy.explain_analyze_level,
        explain_collector=profile.explain_collector if capture_explain else None,
    )


__all__ = ["EngineRuntime", "build_engine_runtime"]
