"""Tests for run helper utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.toolchain import Toolchain
from tools.cq.run.helpers import error_result, merge_in_dir


@dataclass(frozen=True)
class _Context:
    argv: list[str]
    root: Path
    toolchain: Toolchain | None = None


def test_merge_in_dir_combines_run_and_step_scope() -> None:
    """`merge_in_dir` should compose run and step scopes deterministically."""
    assert merge_in_dir("src", "tools/cq") == "src/tools/cq"
    assert merge_in_dir("src", None) == "src"
    assert merge_in_dir(None, "tools/cq") == "tools/cq"


def test_error_result_builds_error_summary_and_finding() -> None:
    """`error_result` should surface exception text in summary and findings."""
    ctx = _Context(argv=["cq", "run"], root=Path())
    result = error_result("step-1", "q", RuntimeError("boom"), ctx)
    assert result.summary.get("error") == "boom"
    assert result.key_findings
    assert result.key_findings[0].message == "step-1: boom"
