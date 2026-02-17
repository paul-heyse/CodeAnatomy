"""Tests for session diagnostics recording adapters."""

from __future__ import annotations

import pytest

from datafusion_engine.session import diagnostics
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from serde_schema_registry import ArtifactSpec


def test_record_runtime_events_noop_without_sink() -> None:
    """Runtime event recorder no-ops when no sink is configured."""
    profile = DataFusionRuntimeProfile()

    diagnostics.record_runtime_events(profile, "rows", [])


def test_record_runtime_artifact_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Artifact recording delegates to global record_artifact helper."""
    profile = DataFusionRuntimeProfile()
    calls: list[tuple[object, object, object]] = []

    def _fake_record_artifact(
        profile_arg: object,
        name: object,
        payload: object,
    ) -> None:
        calls.append((profile_arg, name, payload))

    monkeypatch.setattr(diagnostics, "record_artifact", _fake_record_artifact)

    payload = {"k": "v"}
    diagnostics.record_runtime_artifact(
        profile,
        ArtifactSpec(canonical_name="test.artifact", description="test"),
        payload,
    )

    assert calls
    assert calls[0][0] is profile
    assert calls[0][2] == payload
