"""Tests for driver factory configuration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import pytest

from core_types import JsonValue
from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.driver_factory import (
    ExecutionMode,
    _enforce_policy_validation_result,
    _policy_validation_udf_snapshot,
    _resolve_config_payload,
)
from relspec.errors import RelspecValidationError
from relspec.execution_plan import ExecutionPlan
from relspec.policy_validation import PolicyValidationIssue, PolicyValidationResult


@dataclass(frozen=True)
class _DummySchedule:
    generations: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class _DummyPlan:
    plan_signature: str
    reduced_task_dependency_signature: str
    task_dependency_signature: str
    active_tasks: tuple[str, ...]
    plan_task_signatures: tuple[str, ...]
    task_schedule: _DummySchedule
    reduction_edge_count: int
    reduction_removed_edge_count: int
    session_runtime_hash: str | None
    critical_path_length_weighted: float | None


@dataclass(frozen=True)
class _DummyProfileSpec:
    tracker_config: object | None = None


def _stub_plan() -> ExecutionPlan:
    plan = _DummyPlan(
        plan_signature="plan123",
        reduced_task_dependency_signature="reduced",
        task_dependency_signature="dep",
        active_tasks=("task1", "task2"),
        plan_task_signatures=("sig1",),
        task_schedule=_DummySchedule(generations=(("task1",),)),
        reduction_edge_count=1,
        reduction_removed_edge_count=0,
        session_runtime_hash=None,
        critical_path_length_weighted=None,
    )
    return cast("ExecutionPlan", plan)


def _stub_profile_spec() -> RuntimeProfileSpec:
    return cast("RuntimeProfileSpec", _DummyProfileSpec())


def test_resolve_config_payload_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure config payload defaults are applied consistently."""
    monkeypatch.setenv("CODEANATOMY_RUNTIME_PROFILE", "profile-x")
    payload = _resolve_config_payload(
        {"hamilton": {"tags": {"foo": "bar"}}},
        profile_spec=_stub_profile_spec(),
        plan=_stub_plan(),
        execution_mode=None,
    )
    assert payload["runtime_profile_name_override"] == "profile-x"
    assert payload["enable_dynamic_scan_units"] is True
    hamilton_payload = cast("Mapping[str, JsonValue]", payload["hamilton"])
    tags = cast("Mapping[str, JsonValue]", hamilton_payload["tags"])
    assert tags["plan_signature"] == "plan123"


def test_resolve_config_payload_deterministic_serial_disables_dynamic_scan_units() -> None:
    """Ensure deterministic serial execution disables dynamic scan units."""
    payload = _resolve_config_payload(
        {"enable_dynamic_scan_units": True},
        profile_spec=_stub_profile_spec(),
        plan=_stub_plan(),
        execution_mode=ExecutionMode.DETERMINISTIC_SERIAL,
    )
    assert payload["enable_dynamic_scan_units"] is False


def test_enforce_policy_validation_result_warn_mode_does_not_raise() -> None:
    """Warn mode should not raise even when errors are present."""
    result = PolicyValidationResult(
        issues=(PolicyValidationIssue(code="X", severity="error", detail="failure"),),
    )
    _enforce_policy_validation_result(result=result, mode="warn")


def test_enforce_policy_validation_result_error_mode_raises() -> None:
    """Error mode should raise when error-severity issues are present."""
    result = PolicyValidationResult(
        issues=(PolicyValidationIssue(code="X", severity="error", detail="failure"),),
    )
    with pytest.raises(RelspecValidationError, match="Policy validation failed"):
        _enforce_policy_validation_result(result=result, mode="error")


def test_policy_validation_udf_snapshot_is_deterministic() -> None:
    """UDF snapshot extraction should merge view bundle snapshots deterministically."""

    @dataclass(frozen=True)
    class _Artifacts:
        udf_snapshot: Mapping[str, object]

    @dataclass(frozen=True)
    class _Bundle:
        artifacts: _Artifacts

    @dataclass(frozen=True)
    class _Node:
        name: str
        plan_bundle: _Bundle | None

    @dataclass(frozen=True)
    class _PlanWithViews:
        view_nodes: tuple[_Node, ...]

    plan = cast(
        "ExecutionPlan",
        _PlanWithViews(
            view_nodes=(
                _Node(
                    name="b_view", plan_bundle=_Bundle(artifacts=_Artifacts({"udf_b": object()}))
                ),
                _Node(
                    name="a_view", plan_bundle=_Bundle(artifacts=_Artifacts({"udf_a": object()}))
                ),
            )
        ),
    )
    snapshot = _policy_validation_udf_snapshot(plan)
    assert tuple(snapshot) == ("udf_a", "udf_b")
