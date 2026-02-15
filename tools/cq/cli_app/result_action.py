"""Cyclopts result-action adapter for CQ CLI commands."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from tools.cq.cli_app.context import CliResult, FilterConfig
from tools.cq.cli_app.result import handle_result

ResultActionCallable = Callable[[Any], Any]
ResultActionLiteral = str
ResultAction = (
    ResultActionLiteral
    | ResultActionCallable
    | Sequence[ResultActionLiteral | ResultActionCallable]
)


def _return_int_as_exit_code_else_zero(result: Any) -> int:
    if isinstance(result, bool):
        return 0 if result else 1
    if isinstance(result, int):
        return result
    return 0


def _apply_single_action(result: Any, action: ResultActionLiteral | ResultActionCallable) -> Any:
    if callable(action):
        return action(result)
    if action == "return_int_as_exit_code_else_zero":
        return _return_int_as_exit_code_else_zero(result)
    if action == "return_value":
        return result
    if action == "return_zero":
        return 0
    if action == "return_none":
        return None
    msg = f"Unsupported result_action literal: {action}"
    raise ValueError(msg)


def apply_result_action(result: Any, action: ResultAction) -> Any:
    """Apply a CQ/Cyclopts result-action policy to a command result.

    Returns:
        Any: Processed action output.
    """
    if isinstance(action, Sequence) and not isinstance(action, str):
        current = result
        for part in action:
            current = _apply_single_action(current, part)
        return current
    single_action: ResultActionLiteral | ResultActionCallable = action
    return _apply_single_action(result, single_action)


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


CQ_DEFAULT_RESULT_ACTION: tuple[ResultActionCallable | ResultActionLiteral, ...] = (
    cq_result_action,
    "return_int_as_exit_code_else_zero",
)


__all__ = [
    "CQ_DEFAULT_RESULT_ACTION",
    "apply_result_action",
    "cq_result_action",
]
