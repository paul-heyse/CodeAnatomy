"""Golden snapshot tests for plan output formats."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from cli.commands.plan import _build_payload, _PlanPayloadOptions

if TYPE_CHECKING:
    from hamilton_pipeline.plan_artifacts import PlanArtifactBundle
    from relspec.execution_plan import ExecutionPlan
from tests.cli_golden._support.goldens import assert_text_snapshot


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


def _dummy_plan() -> _DummyPlan:
    return _DummyPlan(
        plan_signature="plan-123",
        reduced_task_dependency_signature="plan-123-reduced",
        active_tasks=["a", "b", "c"],
        dependency_map={"b": ("a",), "c": ("b",)},
        diagnostics=_DummyDiagnostics(dot="digraph { a -> b; b -> c; }"),
        critical_path_task_names=("a", "b", "c"),
        critical_path_length_weighted=3,
    )


def _dummy_bundle() -> _DummyPlanBundle:
    return _DummyPlanBundle(
        schedule_envelope={"schedule": ["a", "b", "c"]},
        validation_envelope={"ok": True},
    )


def _build_text_output() -> str:
    payload = _build_payload(
        plan=cast("ExecutionPlan", _dummy_plan()),
        plan_bundle=cast("PlanArtifactBundle", _dummy_bundle()),
        options=_PlanPayloadOptions(
            show_graph=True,
            show_schedule=True,
            show_inferred_deps=True,
            show_task_graph=True,
            validate=True,
            output_format="text",
        ),
    )
    return str(payload)


def _build_json_output() -> str:
    payload = _build_payload(
        plan=cast("ExecutionPlan", _dummy_plan()),
        plan_bundle=cast("PlanArtifactBundle", _dummy_bundle()),
        options=_PlanPayloadOptions(
            show_graph=True,
            show_schedule=True,
            show_inferred_deps=True,
            show_task_graph=True,
            validate=True,
            output_format="json",
        ),
    )
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _build_dot_output() -> str:
    payload = _build_payload(
        plan=cast("ExecutionPlan", _dummy_plan()),
        plan_bundle=cast("PlanArtifactBundle", _dummy_bundle()),
        options=_PlanPayloadOptions(
            show_graph=True,
            show_schedule=False,
            show_inferred_deps=False,
            show_task_graph=False,
            validate=False,
            output_format="dot",
        ),
    )
    return str(payload) + "\n"


def test_plan_output_text(*, update_golden: bool) -> None:
    """Ensure text plan output matches golden snapshot."""
    assert_text_snapshot("plan_output_text.txt", _build_text_output(), update=update_golden)


def test_plan_output_json(*, update_golden: bool) -> None:
    """Ensure JSON plan output matches golden snapshot."""
    assert_text_snapshot("plan_output_json.txt", _build_json_output(), update=update_golden)


def test_plan_output_dot(*, update_golden: bool) -> None:
    """Ensure DOT plan output matches golden snapshot."""
    assert_text_snapshot("plan_output_dot.txt", _build_dot_output(), update=update_golden)
