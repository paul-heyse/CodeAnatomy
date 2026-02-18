"""Plan-dispatch execution entry points for CQ query runtime."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor_runtime import (
    ExecutePlanRequestV1,
    rg_files_with_matches,
)
from tools.cq.query.executor_runtime import (
    execute_plan as execute_plan_impl,
)
from tools.cq.query.ir import Scope
from tools.cq.search._shared.types import SearchLimits

__all__ = ["ExecutePlanRequestV1", "execute_plan", "rg_files_with_matches"]


def execute_plan(request: ExecutePlanRequestV1, *, tc: Toolchain) -> CqResult:
    """Dispatch a compiled ToolPlan and return result payload.

    Returns:
        CqResult: Result payload produced by the compiled plan.
    """
    return execute_plan_impl(request, tc=tc)


def rg_files_with_matches_for_scope(
    root: Path,
    pattern: str,
    scope: Scope,
    *,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Compatibility helper for scoped ripgrep file discovery.

    Returns:
        list[Path]: Files matching pattern within provided query scope.
    """
    return rg_files_with_matches(root, pattern, scope, limits=limits)
