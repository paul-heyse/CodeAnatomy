"""Runtime-service composition helpers for CLI commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.session.runtime import DataFusionRuntimeProfile

if TYPE_CHECKING:
    from cli.context import RunContext
    from datafusion_engine.delta.service_protocol import DeltaServicePort


@dataclass(frozen=True)
class CliRuntimeServices:
    """Concrete runtime services required by Delta-aware CLI commands."""

    runtime_profile: DataFusionRuntimeProfile
    delta_service: DeltaServicePort


def build_cli_runtime_services(
    *,
    runtime_profile: DataFusionRuntimeProfile,
    delta_service: DeltaServicePort,
) -> CliRuntimeServices:
    """Build concrete runtime services for CLI command execution.

    Returns:
    -------
    CliRuntimeServices
        Runtime services bound to explicit runtime dependencies.
    """
    return CliRuntimeServices(
        runtime_profile=runtime_profile,
        delta_service=delta_service,
    )


def resolve_cli_runtime_services(
    run_context: RunContext | None,
) -> CliRuntimeServices:
    """Resolve runtime services from run context.

    Returns:
    -------
    CliRuntimeServices
        Runtime services from run context.

    Raises:
        RuntimeError: If runtime services are not composed on the run context.
    """
    if run_context is not None and run_context.runtime_services is not None:
        return run_context.runtime_services
    msg = (
        "CliRuntimeServices missing from RunContext. "
        "Delta-aware CLI commands require explicit runtime service composition."
    )
    raise RuntimeError(msg)


__all__ = [
    "CliRuntimeServices",
    "build_cli_runtime_services",
    "resolve_cli_runtime_services",
]
