"""OpenTelemetry config type exports."""

from __future__ import annotations

from obs.otel.bootstrap import OtelBootstrapOptions
from obs.otel.config import OtelConfig, OtelConfigOverrides, OtelConfigSpec

__all__ = [
    "OtelBootstrapOptions",
    "OtelConfig",
    "OtelConfigOverrides",
    "OtelConfigSpec",
]
