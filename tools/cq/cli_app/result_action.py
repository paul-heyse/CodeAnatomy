"""Cyclopts result-action adapter for CQ CLI commands."""

from __future__ import annotations

from typing import Any

from tools.cq.cli_app.context import CliResult, FilterConfig
from tools.cq.cli_app.result import handle_result


def cq_result_action(result: Any) -> int:
    """Normalize CQ command return values to process exit codes.

    Returns:
        int: Normalized exit code for CLI processing.
    """
    if isinstance(result, CliResult):
        return handle_result(result, result.filters or FilterConfig())
    if isinstance(result, int):
        return result
    return 0


__all__ = ["cq_result_action"]
