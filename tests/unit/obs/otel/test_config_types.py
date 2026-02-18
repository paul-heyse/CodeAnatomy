"""Tests for OpenTelemetry config type exports."""

from __future__ import annotations

from obs.otel import config_types


def test_config_types_exports() -> None:
    """Config type module should expose expected OpenTelemetry contracts."""
    assert hasattr(config_types, "OtelConfig")
    assert hasattr(config_types, "OtelConfigSpec")
    assert hasattr(config_types, "OtelConfigOverrides")
