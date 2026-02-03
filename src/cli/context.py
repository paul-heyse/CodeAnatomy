"""Run context for CLI command injection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from core_types import JsonValue

if TYPE_CHECKING:
    from opentelemetry.trace import Span

    from cli.config_source import ConfigWithSources
    from obs.otel import OtelBootstrapOptions


@dataclass(frozen=True)
class RunContext:
    """Injected run context for CLI commands.

    Parameters
    ----------
    run_id
        Run identifier for the CLI invocation.
    log_level
        Logging level applied to the invocation.
    config_contents
        Normalized configuration contents forwarded to the pipeline.
    config_sources
        Optional configuration source metadata for display/debugging.
    span
        Optional root span for telemetry.
    otel_options
        Optional OpenTelemetry bootstrap options.
    """

    run_id: str
    log_level: str
    config_contents: Mapping[str, JsonValue] = field(default_factory=dict)
    config_sources: ConfigWithSources | None = None
    span: Span | None = None
    otel_options: OtelBootstrapOptions | None = None


__all__ = ["RunContext"]
