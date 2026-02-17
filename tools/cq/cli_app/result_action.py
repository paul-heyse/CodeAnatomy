"""Cyclopts result-action adapter for CQ CLI commands."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

from tools.cq.cli_app.context import CliResult, FilterConfig

ResultActionCallable = Callable[[Any], Any]
ResultActionLiteral = str
ResultAction = (
    ResultActionLiteral
    | ResultActionCallable
    | Sequence[ResultActionLiteral | ResultActionCallable]
)


def handle_result(cli_result: CliResult, filters: FilterConfig | None = None) -> int:
    """Handle a CLI result using context-based configuration.

    Returns:
        Normalized process exit code.
    """
    from tools.cq.cli_app.result_filter import apply_result_filters
    from tools.cq.cli_app.result_persist import persist_result_artifacts
    from tools.cq.cli_app.result_render import emit_output, render_result
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.schema import CqResult, assign_result_finding_ids
    from tools.cq.search.pipeline.smart_search import pop_search_object_view_for_run

    non_cq_exit = _handle_non_cq_result(cli_result)
    if non_cq_exit is not None:
        return non_cq_exit

    ctx = cli_result.context
    result = cast("CqResult", cli_result.result)

    output_format = ctx.output_format if ctx.output_format else OutputFormat.md
    artifact_dir = str(ctx.artifact_dir) if ctx.artifact_dir else None
    no_save = not ctx.save_artifact
    resolved_filters = filters if filters is not None else (cli_result.filters or FilterConfig())
    result = apply_result_filters(result, resolved_filters)

    result = assign_result_finding_ids(result)
    result = persist_result_artifacts(
        result=result,
        artifact_dir=artifact_dir,
        no_save=no_save,
        pop_search_object_view_for_run=pop_search_object_view_for_run,
    )
    output = render_result(result, output_format)
    emit_output(output, output_format=output_format)
    return 0


def _handle_non_cq_result(cli_result: CliResult) -> int | None:
    from tools.cq.cli_app.app import console
    from tools.cq.cli_app.context import CliTextResult

    if cli_result.is_cq_result:
        return None
    if isinstance(cli_result.result, CliTextResult):
        if cli_result.result.media_type == "application/json":
            stream = console.file
            stream.write(cli_result.result.text)
            if not cli_result.result.text.endswith("\n"):
                stream.write("\n")
            stream.flush()
        else:
            console.print(cli_result.result.text)
    return cli_result.get_exit_code()


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
    "handle_result",
]
