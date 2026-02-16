"""Session-runtime diagnostics helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    record_artifact,
)
from serde_schema_registry import ArtifactSpec


def record_runtime_artifact(
    profile: DataFusionRuntimeProfile,
    name: ArtifactSpec,
    payload: Mapping[str, object],
) -> None:
    """Record a runtime artifact using the canonical runtime recorder."""
    record_artifact(profile, name, payload)


def record_runtime_events(
    profile: DataFusionRuntimeProfile,
    name: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Record runtime events when diagnostics are configured."""
    from datafusion_engine.lineage.diagnostics import record_events

    record_events(profile, name, rows)


__all__ = ["record_runtime_artifact", "record_runtime_events"]
