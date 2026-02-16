"""Tests for run execution context protocol compatibility."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
from tools.cq.core.run_context import RunExecutionContext
from tools.cq.run.runner import execute_run_plan
from tools.cq.run.spec import RunPlan


@dataclass(frozen=True)
class _Toolchain:
    has_sgpy: bool = False

    @staticmethod
    def to_dict() -> dict[str, str | None]:
        return {}


@dataclass(frozen=True)
class _ExecutionContext:
    root: Path
    argv: list[str]
    toolchain: _Toolchain
    artifact_dir: Path | None = None


def test_execute_run_plan_accepts_protocol_compatible_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run engine should operate with any object matching `RunExecutionContext` protocol."""
    monkeypatch.setenv("CQ_CACHE_ENABLED", "0")
    ctx = _ExecutionContext(root=tmp_path, argv=["cq", "run"], toolchain=_Toolchain())

    result = execute_run_plan(RunPlan(), cast("RunExecutionContext", ctx))

    assert result.summary["plan_version"] == 1
    assert result.run.macro == "run"
