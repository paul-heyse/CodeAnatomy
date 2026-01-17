"""Engine session factory helpers."""

from __future__ import annotations

from dataclasses import replace

from arrowdsl.core.context import ExecutionContext
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime_profile import RuntimeProfileSpec
from engine.session import EngineSession
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.registry import IbisDatasetRegistry
from obs.diagnostics import DiagnosticsCollector


def build_engine_session(
    *,
    ctx: ExecutionContext,
    runtime_spec: RuntimeProfileSpec | None = None,
    diagnostics: DiagnosticsCollector | None = None,
    surface_policy: ExecutionSurfacePolicy | None = None,
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
    df_profile = runtime.datafusion
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


__all__ = ["build_engine_session"]
