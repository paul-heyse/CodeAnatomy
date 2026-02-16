"""Tests for EXPLAIN ANALYZE threshold diagnostics sourced from plan signals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pytest

from datafusion_engine.plan.signals import PlanSignals
from datafusion_engine.views import graph as graph_module

EXPLAIN_DURATION_MS = 25.0
EXPLAIN_OUTPUT_ROWS = 11
from serde_artifact_specs import VIEW_EXPLAIN_ANALYZE_THRESHOLD_SPEC

if TYPE_CHECKING:
    from datafusion_engine.views.graph import ViewGraphContext, ViewNode
else:
    ViewGraphContext = object
    ViewNode = object


@dataclass(frozen=True)
class _Diagnostics:
    explain_analyze_threshold_ms: float | None


@dataclass(frozen=True)
class _Profile:
    diagnostics: _Diagnostics


@dataclass(frozen=True)
class _Runtime:
    runtime_profile: _Profile | None


@dataclass(frozen=True)
class _Context:
    runtime: _Runtime


@dataclass(frozen=True)
class _Bundle:
    plan_fingerprint: str
    plan_identity_hash: str


@dataclass(frozen=True)
class _Node:
    name: str
    plan_bundle: _Bundle | None


def test_maybe_record_explain_analyze_threshold_uses_plan_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Threshold diagnostics should read duration/output rows from plan signals."""
    captured: list[tuple[object, dict[str, Any]]] = []

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        return PlanSignals(explain_analyze_duration_ms=25.0, explain_analyze_output_rows=11)

    def _fake_record_artifact(profile: object, name: object, payload: dict[str, Any]) -> None:
        _ = profile
        captured.append((name, payload))

    monkeypatch.setattr(graph_module, "extract_plan_signals", _fake_extract_plan_signals)
    monkeypatch.setattr(graph_module, "record_artifact", _fake_record_artifact)

    context = _Context(runtime=_Runtime(runtime_profile=_Profile(diagnostics=_Diagnostics(10.0))))
    node = _Node(
        name="view_x",
        plan_bundle=_Bundle(plan_fingerprint="fp:view_x", plan_identity_hash="pid:view_x"),
    )

    graph_module._maybe_record_explain_analyze_threshold(  # noqa: SLF001
        context=cast("ViewGraphContext", context),
        node=cast("ViewNode", node),
    )

    assert len(captured) == 1
    event_name, payload = captured[0]
    assert event_name is VIEW_EXPLAIN_ANALYZE_THRESHOLD_SPEC
    assert payload["duration_ms"] == EXPLAIN_DURATION_MS
    assert payload["output_rows"] == EXPLAIN_OUTPUT_ROWS


def test_maybe_record_explain_analyze_threshold_skips_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duration below threshold should not emit diagnostics artifacts."""
    called = {"recorded": False}

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        return PlanSignals(explain_analyze_duration_ms=5.0, explain_analyze_output_rows=3)

    def _fake_record_artifact(profile: object, name: str, payload: dict[str, Any]) -> None:
        _ = (profile, name, payload)
        called["recorded"] = True

    monkeypatch.setattr(graph_module, "extract_plan_signals", _fake_extract_plan_signals)
    monkeypatch.setattr(graph_module, "record_artifact", _fake_record_artifact)

    context = _Context(runtime=_Runtime(runtime_profile=_Profile(diagnostics=_Diagnostics(10.0))))
    node = _Node(
        name="view_y",
        plan_bundle=_Bundle(plan_fingerprint="fp:view_y", plan_identity_hash="pid:view_y"),
    )

    graph_module._maybe_record_explain_analyze_threshold(  # noqa: SLF001
        context=cast("ViewGraphContext", context),
        node=cast("ViewNode", node),
    )
    assert called["recorded"] is False
