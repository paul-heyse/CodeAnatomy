"""Engine session factory helpers."""

from __future__ import annotations

from dataclasses import replace

from datafusion_engine.dataset.registration import dataset_input_plugin, input_plugin_prefixes
from datafusion_engine.dataset.registry import DatasetCatalog, registry_snapshot
from datafusion_engine.session.runtime import feature_state_snapshot
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime import build_engine_runtime
from engine.runtime_profile import (
    RuntimeProfileSpec,
    engine_runtime_artifact,
    runtime_profile_snapshot,
)
from engine.session import EngineSession
from obs.diagnostics import DiagnosticsCollector
from obs.otel import OtelBootstrapOptions, configure_otel
from relspec.pipeline_policy import DiagnosticsPolicy


def build_engine_session(
    *,
    runtime_spec: RuntimeProfileSpec,
    diagnostics: DiagnosticsCollector | None = None,
    surface_policy: ExecutionSurfacePolicy | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
) -> EngineSession:
    """Build an EngineSession bound to the provided runtime spec.

    Returns
    -------
    EngineSession
        Engine session wired to the runtime surfaces.
    """
    configure_otel(
        service_name="codeanatomy",
        options=OtelBootstrapOptions(
            resource_overrides={"codeanatomy.runtime_profile": runtime_spec.name},
        ),
    )
    engine_runtime = build_engine_runtime(
        runtime_profile=runtime_spec.datafusion,
        diagnostics=diagnostics,
        diagnostics_policy=diagnostics_policy,
    )
    df_profile = engine_runtime.datafusion_profile
    profile_name = runtime_spec.name
    if diagnostics is not None:
        snapshot = feature_state_snapshot(
            profile_name=profile_name,
            determinism_tier=runtime_spec.determinism_tier,
            runtime_profile=df_profile,
        )
        diagnostics.record_events("feature_state_v1", [snapshot.to_row()])
    datasets = DatasetCatalog()
    input_plugin_names: list[str] = []
    if df_profile is not None:
        plugin = dataset_input_plugin(datasets, runtime_profile=df_profile)
        registry_catalogs = dict(df_profile.registry_catalogs)
        registry_catalogs.setdefault(df_profile.default_schema, datasets)
        df_profile = replace(
            df_profile,
            input_plugins=(*df_profile.input_plugins, plugin),
            registry_catalogs=registry_catalogs,
        )
        engine_runtime = engine_runtime.with_datafusion_profile(df_profile)
        input_plugin_names = [plugin.__name__]
    settings_hash = df_profile.settings_hash()
    runtime_snapshot = runtime_profile_snapshot(
        df_profile,
        name=profile_name,
        determinism_tier=runtime_spec.determinism_tier,
    )
    if diagnostics is not None:
        if not diagnostics.artifacts_snapshot().get("engine_runtime_v2"):
            diagnostics.record_artifact(
                "engine_runtime_v2",
                engine_runtime_artifact(
                    df_profile,
                    name=profile_name,
                    determinism_tier=runtime_spec.determinism_tier,
                ),
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
        engine_runtime=engine_runtime,
        datasets=datasets,
        diagnostics=diagnostics,
        surface_policy=surface_policy or ExecutionSurfacePolicy(),
        settings_hash=settings_hash,
        runtime_profile_hash=runtime_snapshot.profile_hash,
    )


__all__ = ["build_engine_session"]
