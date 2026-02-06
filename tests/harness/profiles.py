"""Runtime profile helpers for Delta/DataFusion conformance tests."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    DiagnosticsConfig,
    ExtractOutputConfig,
    FeatureGatesConfig,
)
from obs.diagnostics import DiagnosticsCollector


def conformance_profile(
    *,
    diagnostics: DiagnosticsCollector | None = None,
    dataset_locations: Mapping[str, DatasetLocation] | None = None,
    enforce_native_provider: bool = True,
) -> DataFusionRuntimeProfile:
    """Build a deterministic runtime profile for conformance tests."""
    extract_output = ExtractOutputConfig(dataset_locations=dict(dataset_locations or {}))
    return DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(extract_output=extract_output),
        diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
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
    enforce_native_provider: bool = True,
) -> tuple[DataFusionRuntimeProfile, DiagnosticsCollector]:
    """Build a conformance profile with an attached diagnostics collector."""
    sink = DiagnosticsCollector()
    profile = conformance_profile(
        diagnostics=sink,
        dataset_locations=dataset_locations,
        enforce_native_provider=enforce_native_provider,
    )
    return profile, sink
