"""Tests for run q-step execution helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from tools.cq.core.run_context import RunExecutionContext
from tools.cq.run.q_execution import (
    ParsedQStepLike,
    execute_entity_q_steps,
    execute_pattern_q_steps,
)


@dataclass(frozen=True)
class _Toolchain:
    has_sgpy: bool = False

    @staticmethod
    def to_dict() -> dict[str, str | None]:
        return {}


@dataclass(frozen=True)
class _Context:
    root: Path
    argv: list[str]
    toolchain: _Toolchain
    artifact_dir: Path | None = None


@dataclass(frozen=True)
class _Step:
    parent_step_id: str
    step: object = object()
    query: object = object()
    plan: object = object()
    scope_paths: list[Path] = field(default_factory=list)
    scope_globs: list[str] | None = None


def test_execute_entity_q_steps_returns_error_without_sgpy() -> None:
    """Entity q-steps should fail closed when ast-grep is unavailable."""
    ctx = _Context(root=Path(), argv=["cq", "run"], toolchain=_Toolchain())
    results = execute_entity_q_steps(
        cast("list[ParsedQStepLike]", [_Step(parent_step_id="q_1")]),
        cast("RunExecutionContext", ctx),
        stop_on_error=False,
        run_id="r",
    )

    assert len(results) == 1
    assert "ast-grep not available" in str(results[0][1].summary.get("error", ""))


def test_execute_pattern_q_steps_returns_error_without_sgpy() -> None:
    """Pattern q-steps should fail closed when ast-grep is unavailable."""
    ctx = _Context(root=Path(), argv=["cq", "run"], toolchain=_Toolchain())
    results = execute_pattern_q_steps(
        cast("list[ParsedQStepLike]", [_Step(parent_step_id="q_2")]),
        cast("RunExecutionContext", ctx),
        stop_on_error=False,
        run_id="r",
    )

    assert len(results) == 1
    assert "ast-grep not available" in str(results[0][1].summary.get("error", ""))
