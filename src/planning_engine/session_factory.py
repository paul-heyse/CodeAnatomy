"""Engine session factory helpers."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace as dataclass_replace

import msgspec

from datafusion_engine.dataset.registration import dataset_input_plugin, input_plugin_prefixes
from datafusion_engine.dataset.registry import DatasetCatalog, registry_snapshot
from datafusion_engine.materialize_policy import MaterializationPolicy
from datafusion_engine.session.runtime import feature_state_snapshot
from obs.diagnostics import DiagnosticsCollector
from obs.otel import OtelBootstrapOptions, configure_otel
from planning_engine.runtime import build_engine_runtime
from planning_engine.runtime_profile import (
    RuntimeProfileSpec,
    engine_runtime_artifact,
    runtime_profile_snapshot,
)
from planning_engine.session import EngineSession
from relspec.pipeline_policy import DiagnosticsPolicy


@dataclass(frozen=True)
class SemanticBuildOptions:
    """Options for semantic build operations.

    Attributes:
    ----------
    use_cdf
        Whether to enable CDF-aware incremental joins.
    validate_inputs
        Whether to validate input schemas.
    collect_metrics
        Whether to collect operation metrics.
    trace_fingerprints
        Whether to trace plan fingerprints.
    """

    use_cdf: bool = False
    validate_inputs: bool = True
    collect_metrics: bool = True
    trace_fingerprints: bool = True


@dataclass(frozen=True)
class EngineSessionOptions:
    """Options for constructing an EngineSession."""

    diagnostics: DiagnosticsCollector | None = None
    surface_policy: MaterializationPolicy | None = None
    diagnostics_policy: DiagnosticsPolicy | None = None
    build_options: SemanticBuildOptions | None = None
    otel_options: OtelBootstrapOptions | None = None


def build_engine_session(
    *,
    runtime_spec: RuntimeProfileSpec,
    options: EngineSessionOptions | None = None,
) -> EngineSession:
    """Build an EngineSession bound to the provided runtime spec.

    Parameters
    ----------
    runtime_spec
        Resolved runtime profile specification.
    options
        Optional engine session options for diagnostics and tracing.

    Returns:
    -------
    EngineSession
        Engine session wired to the runtime surfaces.
    """
    resolved = options or EngineSessionOptions()
    _ = resolved.build_options  # Reserved for future use
    effective_otel = resolved.otel_options or OtelBootstrapOptions()
    resource_overrides = dict(effective_otel.resource_overrides or {})
    resource_overrides["codeanatomy.runtime_profile"] = runtime_spec.name
    configure_otel(
        service_name="codeanatomy",
        options=dataclass_replace(effective_otel, resource_overrides=resource_overrides),
    )
    engine_runtime = build_engine_runtime(
        runtime_profile=runtime_spec.datafusion,
        diagnostics=resolved.diagnostics,
        diagnostics_policy=resolved.diagnostics_policy,
    )
    df_profile = engine_runtime.datafusion_profile
    profile_name = runtime_spec.name
    if resolved.diagnostics is not None:
        snapshot = feature_state_snapshot(
            profile_name=profile_name,
            determinism_tier=runtime_spec.determinism_tier,
            runtime_profile=df_profile,
        )
        resolved.diagnostics.record_events("feature_state_v1", [snapshot.to_row()])
    datasets = DatasetCatalog()
    input_plugin_names: list[str] = []
    if df_profile is not None:
        plugin = dataset_input_plugin(datasets, runtime_profile=df_profile)
        registry_catalogs = dict(df_profile.catalog.registry_catalogs)
        registry_catalogs.setdefault(df_profile.catalog.default_schema, datasets)
        df_profile = msgspec.structs.replace(
            df_profile,
            policies=msgspec.structs.replace(
                df_profile.policies,
                input_plugins=(*df_profile.policies.input_plugins, plugin),
            ),
            catalog=msgspec.structs.replace(
                df_profile.catalog,
                registry_catalogs=registry_catalogs,
            ),
        )
        engine_runtime = engine_runtime.with_datafusion_profile(df_profile)
        input_plugin_names = [plugin.__name__]
    settings_hash = df_profile.settings_hash()
    runtime_snapshot = runtime_profile_snapshot(
        df_profile,
        name=profile_name,
        determinism_tier=runtime_spec.determinism_tier,
    )
    if resolved.diagnostics is not None:
        from serde_artifact_specs import DATAFUSION_INPUT_PLUGINS_SPEC, ENGINE_RUNTIME_SPEC

        if not resolved.diagnostics.artifacts_snapshot().get("engine_runtime_v2"):
            resolved.diagnostics.record_artifact(
                ENGINE_RUNTIME_SPEC,
                engine_runtime_artifact(
                    df_profile,
                    name=profile_name,
                    determinism_tier=runtime_spec.determinism_tier,
                ),
            )
        resolved.diagnostics.record_artifact(
            DATAFUSION_INPUT_PLUGINS_SPEC,
            {
                "plugins": input_plugin_names,
                "prefixes": list(input_plugin_prefixes()),
                "dataset_registry": registry_snapshot(datasets),
            },
        )
    return EngineSession(
        engine_runtime=engine_runtime,
        datasets=datasets,
        diagnostics=resolved.diagnostics,
        surface_policy=resolved.surface_policy or MaterializationPolicy(),
        settings_hash=settings_hash,
        runtime_profile_hash=runtime_snapshot.profile_hash,
    )


__all__ = ["EngineSessionOptions", "SemanticBuildOptions", "build_engine_session"]
