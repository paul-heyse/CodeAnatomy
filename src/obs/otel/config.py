"""Compatibility facade for OpenTelemetry configuration APIs."""

from __future__ import annotations

from obs.otel.config_resolution import resolve_otel_config
from obs.otel.config_types import (
    BatchProcessorSettings,
    OtelConfig,
    OtelConfigOverrides,
    OtelConfigSpec,
)

__all__ = [
    "BatchProcessorSettings",
    "OtelConfig",
    "OtelConfigOverrides",
    "OtelConfigSpec",
    "resolve_otel_config",
]
