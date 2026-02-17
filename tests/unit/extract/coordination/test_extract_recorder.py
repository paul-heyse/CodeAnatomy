"""Tests for extract artifact recorder helper."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from extract.coordination.extract_recorder import record_extract_artifact
from serde_artifact_specs import PLAN_EXECUTE_SPEC

if TYPE_CHECKING:
    from extraction.diagnostics import EngineEventRecorder
    from serde_schema_registry import ArtifactSpec


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[ArtifactSpec, dict[str, object]]] = []

    def record_artifact(self, name: ArtifactSpec, payload: dict[str, object]) -> None:
        self.calls.append((name, payload))


def test_record_extract_artifact() -> None:
    """Recorder helper should forward event name and payload."""
    recorder = _Recorder()
    record_extract_artifact(
        cast("EngineEventRecorder", recorder),
        name=PLAN_EXECUTE_SPEC,
        payload={"k": "v"},
    )
    assert recorder.calls == [(PLAN_EXECUTE_SPEC, {"k": "v"})]
