"""Tests for driver factory configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from engine.runtime_profile import RuntimeProfileSpec
from hamilton_pipeline.driver_factory import ExecutionMode, _resolve_config_payload
from relspec.execution_plan import ExecutionPlan


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
        {"hamilton_tags": {"foo": "bar"}},
        profile_spec=_stub_profile_spec(),
        plan=_stub_plan(),
        execution_mode=None,
    )
    assert payload["runtime_profile_name_override"] == "profile-x"
    assert payload["enable_dynamic_scan_units"] is True
    tags = payload["hamilton_tags"]
    assert isinstance(tags, dict)
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
