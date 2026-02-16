"""Shared helpers for CQ run execution modules."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from tools.cq.core.result_factory import build_error_result
from tools.cq.core.schema import CqResult, Finding, assign_result_finding_ids, ms

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain


class RunContextLike(Protocol):
    """Minimal context required by run helper utilities."""

    @property
    def argv(self) -> list[str]:
        """Return CLI argv tokens for the run context."""
        ...

    @property
    def root(self) -> Path:
        """Return repository root path for run execution."""
        ...

    @property
    def toolchain(self) -> Toolchain | None:
        """Return detected toolchain information when available."""
        ...


def error_result(step_id: str, macro: str, exc: Exception, ctx: RunContextLike) -> CqResult:
    """Build a standard run-step error result.

    Returns:
        CqResult: Error result with summary and step-scoped finding.
    """
    result = build_error_result(
        macro=macro,
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
        started_ms=ms(),
        error=exc,
    )
    result.key_findings.append(
        Finding(category="error", message=f"{step_id}: {exc}", severity="error")
    )
    assign_result_finding_ids(result)
    return result


def merge_in_dir(run_in_dir: str | None, step_in_dir: str | None) -> str | None:
    """Merge run-level and step-level in_dir scopes.

    Returns:
        str | None: Combined scope path or the remaining non-empty scope.
    """
    if run_in_dir and step_in_dir:
        return str(Path(run_in_dir) / step_in_dir)
    return step_in_dir or run_in_dir


__all__ = [
    "RunContextLike",
    "error_result",
    "merge_in_dir",
]
