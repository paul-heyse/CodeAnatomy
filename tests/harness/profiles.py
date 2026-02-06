"""Runtime profile helpers for Delta/DataFusion conformance tests."""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    DiagnosticsConfig,
    ExtractOutputConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from obs.diagnostics import DiagnosticsCollector


def conformance_profile(
    *,
    diagnostics: DiagnosticsCollector | None = None,
    dataset_locations: Mapping[str, DatasetLocation] | None = None,
    enforce_native_provider: bool | None = None,
    plan_artifacts_root: str | None = None,
) -> DataFusionRuntimeProfile:
    """Build a deterministic runtime profile for conformance tests.

    Returns:
    -------
    DataFusionRuntimeProfile
        Runtime profile configured for conformance scenarios.
    """
    if enforce_native_provider is None:
        enforce_native_provider = strict_native_provider_supported()
    extract_output = ExtractOutputConfig(dataset_locations=dict(dataset_locations or {}))
    policies = (
        PolicyBundleConfig(plan_artifacts_root=plan_artifacts_root)
        if plan_artifacts_root is not None
        else PolicyBundleConfig()
    )
    return DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(extract_output=extract_output),
        diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
        policies=policies,
        features=FeatureGatesConfig(
            enable_schema_registry=False,
            enable_schema_evolution_adapter=False,
            enable_udfs=False,
            enforce_delta_ffi_provider=enforce_native_provider,
        ),
    )


def conformance_profile_with_sink(
    *,
    dataset_locations: Mapping[str, DatasetLocation] | None = None,
    enforce_native_provider: bool | None = None,
    plan_artifacts_root: str | None = None,
) -> tuple[DataFusionRuntimeProfile, DiagnosticsCollector]:
    """Build a conformance profile with an attached diagnostics collector.

    Returns:
    -------
    tuple[DataFusionRuntimeProfile, DiagnosticsCollector]
        The configured runtime profile and its diagnostics sink.
    """
    sink = DiagnosticsCollector()
    profile = conformance_profile(
        diagnostics=sink,
        dataset_locations=dataset_locations,
        enforce_native_provider=enforce_native_provider,
        plan_artifacts_root=plan_artifacts_root,
    )
    return profile, sink


@lru_cache(maxsize=1)
def strict_native_provider_supported() -> bool:
    """Return whether strict non-fallback Delta provider probing is compatible.

    Returns:
    -------
    bool
        `True` when strict native provider probing is both available and compatible.
    """
    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            enable_schema_registry=False,
            enable_schema_evolution_adapter=False,
            enable_udfs=False,
            enforce_delta_ffi_provider=True,
        ),
    )
    compatibility = is_delta_extension_compatible(
        profile.session_context(),
        entrypoint="delta_table_provider_from_session",
        require_non_fallback=True,
    )
    return compatibility.available and compatibility.compatible
