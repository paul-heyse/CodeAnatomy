"""Engine session factory helpers."""

from __future__ import annotations

from dataclasses import replace

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.dataset_registry import DatasetCatalog, registry_snapshot
from datafusion_engine.registry_bridge import dataset_input_plugin, input_plugin_prefixes
from datafusion_engine.runtime import feature_state_snapshot
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime import build_engine_runtime
from engine.runtime_profile import (
    RuntimeProfileSpec,
    engine_runtime_artifact,
    runtime_profile_snapshot,
)
from engine.session import EngineSession
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
    runtime_profile = ctx.runtime if runtime_spec is None else runtime_spec.runtime
    engine_runtime = build_engine_runtime(
        ctx=ctx,
        runtime_profile=runtime_profile,
        diagnostics=diagnostics,
        diagnostics_policy=diagnostics_policy,
    )
    runtime = engine_runtime.runtime_profile
    df_profile = engine_runtime.datafusion_profile
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
    datasets = DatasetCatalog()
    input_plugin_names: list[str] = []
    if df_profile is not None:
        plugin = dataset_input_plugin(datasets, runtime_profile=df_profile)
        df_profile = replace(
            df_profile,
            input_plugins=(*df_profile.input_plugins, plugin),
        )
        runtime = runtime.with_datafusion(df_profile)
        engine_runtime = engine_runtime.with_datafusion_profile(df_profile)
        input_plugin_names = [plugin.__name__]
    settings_hash = df_profile.settings_hash() if df_profile is not None else None
    runtime_snapshot = runtime_profile_snapshot(runtime)
    if diagnostics is not None:
        if not diagnostics.artifacts_snapshot().get("engine_runtime_v1"):
            diagnostics.record_artifact(
                "engine_runtime_v1",
                engine_runtime_artifact(runtime),
            )
        diagnostics.record_artifact(
            "datafusion_input_plugins_v1",
            {
                "plugins": input_plugin_names,
                "prefixes": list(input_plugin_prefixes()),
                "dataset_registry": registry_snapshot(datasets),
            },
        )
    return EngineSession(
        ctx=ctx,
        engine_runtime=engine_runtime,
        datasets=datasets,
        diagnostics=diagnostics,
        surface_policy=surface_policy or ExecutionSurfacePolicy(),
        settings_hash=settings_hash,
        runtime_profile_hash=runtime_snapshot.profile_hash,
    )


__all__ = ["build_engine_session"]
