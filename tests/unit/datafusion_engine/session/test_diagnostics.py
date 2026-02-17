# ruff: noqa: D100, D103, ANN001, ANN202
from __future__ import annotations

from datafusion_engine.session import diagnostics
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from serde_schema_registry import ArtifactSpec


def test_record_runtime_events_noop_without_sink() -> None:
    profile = DataFusionRuntimeProfile()

    diagnostics.record_runtime_events(profile, "rows", [])


def test_record_runtime_artifact_delegates(monkeypatch) -> None:
    profile = DataFusionRuntimeProfile()
    calls: list[tuple[object, object, object]] = []

    def _fake_record_artifact(profile_arg, name, payload):
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
