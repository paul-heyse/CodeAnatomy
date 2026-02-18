"""Protocol contracts for DataFusion session runtime surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable


@runtime_checkable
class RuntimeSettingsProvider(Protocol):
    """Protocol for profile settings serialization."""

    def settings_payload(self) -> dict[str, str]:
        """Return runtime settings payload."""
        ...


@runtime_checkable
class RuntimeTelemetryProvider(Protocol):
    """Protocol for runtime telemetry serialization."""

    def telemetry_payload(self) -> dict[str, object]:
        """Return telemetry payload."""
        ...

    def telemetry_payload_v1(self) -> dict[str, object]:
        """Return v1 telemetry payload."""
        ...


@runtime_checkable
class RuntimeArtifactRecorder(Protocol):
    """Protocol for artifact recording hooks used by runtime flows."""

    def record_artifact(self, name: object, payload: Mapping[str, object]) -> None:
        """Record runtime artifact payload."""
        ...


@runtime_checkable
class PlannerExtensionPort(Protocol):
    """Protocol for custom planner extension installers."""

    def install_expr_planners(self, ctx: object) -> None:
        """Install expression planners into ``ctx``."""
        ...

    def install_relation_planner(self, ctx: object) -> None:
        """Install relation planner into ``ctx``."""
        ...


__all__ = [
    "PlannerExtensionPort",
    "RuntimeArtifactRecorder",
    "RuntimeSettingsProvider",
    "RuntimeTelemetryProvider",
]
