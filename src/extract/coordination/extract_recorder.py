"""Extraction diagnostics recording helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from extraction.diagnostics import EngineEventRecorder

if TYPE_CHECKING:
    from serde_schema_registry import ArtifactSpec


def record_extract_artifact(
    recorder: EngineEventRecorder | None,
    *,
    name: ArtifactSpec,
    payload: Mapping[str, object],
) -> None:
    """Record extraction artifact payload when recorder is available."""
    if recorder is None:
        return
    recorder.record_artifact(name, dict(payload))


__all__ = ["record_extract_artifact"]
