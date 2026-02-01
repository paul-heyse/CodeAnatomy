"""Engine session factory helpers."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from datafusion_engine.dataset.registration import dataset_input_plugin, input_plugin_prefixes
from datafusion_engine.dataset.registry import DatasetCatalog, registry_snapshot
from datafusion_engine.materialize_policy import MaterializationPolicy
from datafusion_engine.semantics_runtime import (
    apply_semantic_runtime_config,
    semantic_runtime_from_profile,
)
from datafusion_engine.session.runtime import feature_state_snapshot
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

if TYPE_CHECKING:
    from semantics.runtime import SemanticBuildOptions, SemanticRuntimeConfig


def build_engine_session(  # noqa: PLR0913
    *,
    runtime_spec: RuntimeProfileSpec,
    diagnostics: DiagnosticsCollector | None = None,
    surface_policy: MaterializationPolicy | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
    semantic_config: SemanticRuntimeConfig | None = None,
    build_options: SemanticBuildOptions | None = None,
) -> EngineSession:
    """Build an EngineSession bound to the provided runtime spec.

    Parameters
    ----------
    runtime_spec
        Resolved runtime profile specification.
    diagnostics
        Optional diagnostics collector for recording events.
    surface_policy
        Optional materialization policy override.
    diagnostics_policy
        Optional diagnostics policy configuration.
    semantic_config
        Optional semantic runtime configuration. When provided, takes precedence
        over values inferred from the runtime profile.
    build_options
        Optional semantic build options (reserved for future use).

    Returns
    -------
    EngineSession
        Engine session wired to the runtime surfaces.
    """
    _ = build_options  # Reserved for future use
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

    # Resolve and apply semantic config via the datafusion_engine bridge
    resolved_semantic = semantic_config or semantic_runtime_from_profile(df_profile)
    df_profile = apply_semantic_runtime_config(df_profile, resolved_semantic)
    engine_runtime = engine_runtime.with_datafusion_profile(df_profile)
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
        surface_policy=surface_policy or MaterializationPolicy(),
        settings_hash=settings_hash,
        runtime_profile_hash=runtime_snapshot.profile_hash,
    )


__all__ = ["build_engine_session"]
