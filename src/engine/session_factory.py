"""Engine session factory helpers."""

from __future__ import annotations

from dataclasses import replace

from arrowdsl.core.context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile, feature_state_snapshot
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime_profile import RuntimeProfileSpec
from engine.session import EngineSession
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.registry import IbisDatasetRegistry
from obs.diagnostics import DiagnosticsCollector
from relspec.pipeline_policy import DiagnosticsPolicy


def build_engine_session(
    *,
    ctx: ExecutionContext,
    runtime_spec: RuntimeProfileSpec | None = None,
    diagnostics: DiagnosticsCollector | None = None,
    surface_policy: ExecutionSurfacePolicy | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
) -> EngineSession:
    """Build an EngineSession bound to the provided ExecutionContext.

    Returns
    -------
    EngineSession
        Engine session wired to the runtime surfaces.
    """
    if runtime_spec is None:
        runtime = ctx.runtime
        ibis_fuse_selects = ctx.runtime.ibis_fuse_selects
    else:
        runtime = runtime_spec.runtime
        ibis_fuse_selects = runtime_spec.ibis_fuse_selects
    runtime.apply_global_thread_pools()
    df_profile = runtime.datafusion
    if df_profile is not None and diagnostics_policy is not None:
        df_profile = _apply_diagnostics_policy(df_profile, diagnostics_policy)
    if df_profile is not None and diagnostics is not None:
        df_profile = replace(df_profile, diagnostics_sink=diagnostics)
        runtime = runtime.with_datafusion(df_profile)
    ctx = ExecutionContext(
        runtime=runtime,
        mode=ctx.mode,
        provenance=ctx.provenance,
        safe_cast=ctx.safe_cast,
        debug=ctx.debug,
        schema_validation=ctx.schema_validation,
    )
    profile_name = runtime_spec.name if runtime_spec is not None else runtime.name
    if diagnostics is not None:
        snapshot = feature_state_snapshot(
            profile_name=profile_name,
            determinism_tier=runtime.determinism,
            runtime_profile=df_profile,
        )
        diagnostics.record_events("feature_state_v1", [snapshot.to_row()])
    backend_cfg = IbisBackendConfig(
        datafusion_profile=df_profile,
        fuse_selects=ibis_fuse_selects,
    )
    backend = build_backend(backend_cfg)
    datasets = IbisDatasetRegistry(backend=backend, runtime_profile=df_profile)
    settings_hash = df_profile.settings_hash() if df_profile is not None else None
    return EngineSession(
        ctx=ctx,
        runtime_profile=runtime,
        df_profile=df_profile,
        ibis_backend=backend,
        datasets=datasets,
        diagnostics=diagnostics,
        surface_policy=surface_policy or ExecutionSurfacePolicy(),
        settings_hash=settings_hash,
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
    capture_fallbacks = policy.capture_datafusion_fallbacks
    return replace(
        profile,
        capture_explain=capture_explain,
        explain_analyze=policy.explain_analyze,
        explain_analyze_level=policy.explain_analyze_level,
        explain_collector=profile.explain_collector if capture_explain else None,
        capture_fallbacks=capture_fallbacks,
        fallback_collector=profile.fallback_collector if capture_fallbacks else None,
    )


__all__ = ["build_engine_session"]
