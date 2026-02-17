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
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> CliRuntimeServices:
    """Build concrete runtime services for CLI command execution.

    Returns:
    -------
    CliRuntimeServices
        Runtime services bound to the provided or default profile.
    """
    profile = runtime_profile or DataFusionRuntimeProfile()
    return CliRuntimeServices(
        runtime_profile=profile,
        delta_service=profile.delta_ops.delta_service(),
    )


def resolve_cli_runtime_services(
    run_context: RunContext | None,
) -> CliRuntimeServices:
    """Resolve runtime services from run context or create command-local defaults.

    Returns:
    -------
    CliRuntimeServices
        Runtime services from context when available, otherwise local defaults.
    """
    if run_context is not None and run_context.runtime_services is not None:
        return run_context.runtime_services
    return build_cli_runtime_services()


__all__ = [
    "CliRuntimeServices",
    "build_cli_runtime_services",
    "resolve_cli_runtime_services",
]
