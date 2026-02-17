# ruff: noqa: D100, D103
from __future__ import annotations

from datafusion_engine.session.protocols import (
    RuntimeSettingsProvider,
    RuntimeTelemetryProvider,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_runtime_profile_conforms_to_settings_protocol() -> None:
    profile = DataFusionRuntimeProfile()
    assert isinstance(profile, RuntimeSettingsProvider)


def test_runtime_profile_conforms_to_telemetry_protocol() -> None:
    profile = DataFusionRuntimeProfile()
    assert isinstance(profile, RuntimeTelemetryProvider)


def test_runtime_profile_exposes_record_artifact_method() -> None:
    profile = DataFusionRuntimeProfile()
    assert callable(getattr(profile, "record_artifact", None))
