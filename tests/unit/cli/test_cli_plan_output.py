"""Tests for plan command output formatting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from cli.commands.plan import _build_payload, _PlanPayloadOptions

if TYPE_CHECKING:
    from hamilton_pipeline.plan_artifacts import PlanArtifactBundle
    from relspec.execution_plan import ExecutionPlan


@dataclass(frozen=True)
class _DummyDiagnostics:
    dot: str


@dataclass(frozen=True)
class _DummyPlan:
    plan_signature: str
    reduced_task_dependency_signature: str
    active_tasks: list[str]
    dependency_map: dict[str, tuple[str, ...]]
    diagnostics: _DummyDiagnostics
    critical_path_task_names: tuple[str, ...]
    critical_path_length_weighted: int


@dataclass(frozen=True)
class _DummyPlanBundle:
    schedule_envelope: dict[str, object]
    validation_envelope: dict[str, object]


def _build_dummy_plan() -> _DummyPlan:
    return _DummyPlan(
        plan_signature="plan-123",
        reduced_task_dependency_signature="plan-123-reduced",
        active_tasks=["a", "b", "c"],
        dependency_map={
            "b": ("a",),
            "c": ("b",),
        },
        diagnostics=_DummyDiagnostics(dot="digraph { a -> b; b -> c; }"),
        critical_path_task_names=("a", "b", "c"),
        critical_path_length_weighted=3,
    )


def _build_dummy_bundle() -> _DummyPlanBundle:
    return _DummyPlanBundle(
        schedule_envelope={"schedule": ["a", "b", "c"]},
        validation_envelope={"ok": True},
    )


def test_build_payload_json_includes_sections() -> None:
    """Ensure JSON payload includes requested sections."""
    plan = _build_dummy_plan()
    bundle = _build_dummy_bundle()

    payload = _build_payload(
        plan=cast("ExecutionPlan", plan),
        plan_bundle=cast("PlanArtifactBundle", bundle),
        options=_PlanPayloadOptions(
            show_graph=True,
            show_schedule=True,
            show_inferred_deps=True,
            show_task_graph=True,
            validate=True,
            output_format="json",
        ),
    )

    assert isinstance(payload, dict)
    assert payload["plan_signature"] == "plan-123"
    assert payload["reduced_plan_signature"] == "plan-123-reduced"
    assert payload["task_count"] == 3
    assert "graph" in payload
    assert "schedule" in payload
    assert "validation" in payload
    assert "inferred_deps" in payload
    assert "task_graph" in payload


def test_build_payload_text_includes_task_graph() -> None:
    """Ensure text payload includes task graph summary lines."""
    plan = _build_dummy_plan()
    bundle = _build_dummy_bundle()

    payload = _build_payload(
        plan=cast("ExecutionPlan", plan),
        plan_bundle=cast("PlanArtifactBundle", bundle),
        options=_PlanPayloadOptions(
            show_graph=True,
            show_schedule=True,
            show_inferred_deps=True,
            show_task_graph=True,
            validate=True,
            output_format="text",
        ),
    )

    assert isinstance(payload, str)
    assert "plan_signature: plan-123" in payload
    assert "task_graph:" in payload
    assert "node_count: 3" in payload
    assert "edge_count: 2" in payload


def test_build_payload_dot_returns_dot() -> None:
    """Ensure dot output returns the DOT graph string."""
    plan = _build_dummy_plan()
    bundle = _build_dummy_bundle()

    payload = _build_payload(
        plan=cast("ExecutionPlan", plan),
        plan_bundle=cast("PlanArtifactBundle", bundle),
        options=_PlanPayloadOptions(
            show_graph=True,
            show_schedule=False,
            show_inferred_deps=False,
            show_task_graph=False,
            validate=False,
            output_format="dot",
        ),
    )

    assert payload == "digraph { a -> b; b -> c; }"
