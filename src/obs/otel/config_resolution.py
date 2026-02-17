"""OpenTelemetry config resolution helpers."""

from __future__ import annotations

from obs.otel.config import OtelConfig, OtelConfigOverrides, OtelConfigSpec, resolve_otel_config


def resolve_otel_config_spec(
    spec: OtelConfigSpec | None = None,
    overrides: OtelConfigOverrides | None = None,
) -> OtelConfig:
    """Resolve OpenTelemetry runtime config from spec and overrides.

    Returns:
        OtelConfig: Effective OpenTelemetry configuration.
    """
    return resolve_otel_config(spec=spec, overrides=overrides)


__all__ = ["resolve_otel_config_spec"]
