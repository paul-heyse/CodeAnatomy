"""Tests for runtime protocol conformance on session profile."""

from __future__ import annotations

from datafusion_engine.session.protocols import (
    RuntimeSettingsProvider,
    RuntimeTelemetryProvider,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def test_runtime_profile_conforms_to_settings_protocol() -> None:
    """Runtime profile conforms to runtime settings protocol."""
    profile = DataFusionRuntimeProfile()
    assert isinstance(profile, RuntimeSettingsProvider)


def test_runtime_profile_conforms_to_telemetry_protocol() -> None:
    """Runtime profile conforms to runtime telemetry protocol."""
    profile = DataFusionRuntimeProfile()
    assert isinstance(profile, RuntimeTelemetryProvider)


def test_runtime_profile_exposes_record_artifact_method() -> None:
    """Runtime profile exposes artifact recording method."""
    profile = DataFusionRuntimeProfile()
    assert callable(getattr(profile, "record_artifact", None))
