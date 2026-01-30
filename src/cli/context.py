"""Run context for CLI command injection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from opentelemetry.trace import Span

    from core_types import JsonValue


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
    span
        Optional root span for telemetry.
    """

    run_id: str
    log_level: str
    config_contents: Mapping[str, JsonValue] = field(default_factory=dict)
    span: Span | None = None


__all__ = ["RunContext"]
