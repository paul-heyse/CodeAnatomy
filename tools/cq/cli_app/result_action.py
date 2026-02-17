"""Cyclopts result-action adapter for CQ CLI commands."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from tools.cq.cli_app.context import CliResult, FilterConfig

if TYPE_CHECKING:
    from tools.cq.cli_app.protocols import ConsolePort
    from tools.cq.cli_app.types import OutputFormat

ResultActionCallable = Callable[[Any], Any]
ResultActionLiteral = str
ResultAction = (
    ResultActionLiteral
    | ResultActionCallable
    | Sequence[ResultActionLiteral | ResultActionCallable]
)


@dataclass(frozen=True, slots=True)
class PreparedOutput:
    """Pure output payload prepared for emission."""

    exit_code: int
    output: str | None = None
    output_format: OutputFormat | None = None
    emit_raw: bool = False
    console: ConsolePort | None = None


def prepare_output(
    cli_result: CliResult,
    filters: FilterConfig | None = None,
) -> PreparedOutput:
    """Prepare output payload for a CLI result without side effects.

    Returns:
        PreparedOutput: Prepared output payload for command emission.
    """
    from tools.cq.cli_app.result_filter import apply_result_filters
    from tools.cq.cli_app.result_persist import persist_result_artifacts
    from tools.cq.cli_app.result_render import render_result
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.schema import CqResult, assign_result_finding_ids
    from tools.cq.search.pipeline.smart_search import pop_search_object_view_for_run

    non_cq_output = _prepare_non_cq_output(cli_result)
    if non_cq_output is not None:
        return non_cq_output

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
    return PreparedOutput(
        exit_code=0,
        output=output,
        output_format=output_format,
        console=ctx.console,
    )


def emit_prepared_output(prepared: PreparedOutput) -> int:
    """Emit one prepared output payload to configured output streams.

    Returns:
    -------
    int
        Process exit code for the emitted output.
    """
    from tools.cq.cli_app.result_render import emit_output
    from tools.cq.cli_app.types import OutputFormat

    if prepared.output is None:
        return prepared.exit_code

    console = prepared.console
    if console is None:
        from rich.console import Console

        console = Console(highlight=False, stderr=False)

    if prepared.emit_raw:
        stream = console.file
        stream.write(prepared.output)
        if not prepared.output.endswith("\n"):
            stream.write("\n")
        stream.flush()
        return prepared.exit_code

    output_format = prepared.output_format
    if not isinstance(output_format, OutputFormat):
        output_format = OutputFormat.md
    emit_output(prepared.output, output_format=output_format, console=cast("ConsolePort", console))
    return prepared.exit_code


def _prepare_non_cq_output(cli_result: CliResult) -> PreparedOutput | None:
    from tools.cq.cli_app.context import CliTextResult
    from tools.cq.cli_app.types import OutputFormat

    if cli_result.is_cq_result:
        return None
    if isinstance(cli_result.result, CliTextResult):
        is_json = cli_result.result.media_type == "application/json"
        return PreparedOutput(
            exit_code=cli_result.get_exit_code(),
            output=cli_result.result.text,
            output_format=OutputFormat.json if is_json else OutputFormat.md,
            emit_raw=is_json,
            console=cli_result.context.console,
        )
    return PreparedOutput(exit_code=cli_result.get_exit_code())


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
        prepared = prepare_output(result, result.filters or FilterConfig())
        return emit_prepared_output(prepared)
    if isinstance(result, int):
        return result
    return 0


CQ_DEFAULT_RESULT_ACTION: tuple[ResultActionCallable | ResultActionLiteral, ...] = (
    cq_result_action,
    "return_int_as_exit_code_else_zero",
)


__all__ = [
    "CQ_DEFAULT_RESULT_ACTION",
    "PreparedOutput",
    "apply_result_action",
    "cq_result_action",
    "emit_prepared_output",
    "prepare_output",
]
