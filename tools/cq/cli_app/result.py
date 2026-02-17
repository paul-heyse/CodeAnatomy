"""Result orchestration for CQ CLI output handling."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from tools.cq.cli_app.result_filter import apply_result_filters
from tools.cq.cli_app.result_persist import persist_result_artifacts
from tools.cq.cli_app.result_render import emit_output, render_result

if TYPE_CHECKING:
    from tools.cq.cli_app.context import CliResult, FilterConfig
    from tools.cq.core.schema import CqResult


def handle_result(cli_result: CliResult, filters: FilterConfig | None = None) -> int:
    """Handle a CLI result using context-based configuration.

    Returns:
        int: Process exit code.
    """
    from tools.cq.cli_app.context import FilterConfig
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.search.pipeline.smart_search import pop_search_object_view_for_run

    non_cq_exit = _handle_non_cq_result(cli_result)
    if non_cq_exit is not None:
        return non_cq_exit

    ctx = cli_result.context
    result = cast("CqResult", cli_result.result)

    output_format = ctx.output_format if ctx.output_format else OutputFormat.md
    artifact_dir = str(ctx.artifact_dir) if ctx.artifact_dir else None
    no_save = not ctx.save_artifact

    resolved_filters = _resolve_filters(cli_result, filters, FilterConfig)
    result = apply_result_filters(result, resolved_filters)

    from tools.cq.core.schema import assign_result_finding_ids

    result = assign_result_finding_ids(result)

    persist_result_artifacts(
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


def _resolve_filters(
    cli_result: CliResult,
    filters: FilterConfig | None,
    filter_type: type[FilterConfig],
) -> FilterConfig:
    if filters is not None:
        return filters
    return cli_result.filters if cli_result.filters else filter_type()


__all__ = ["handle_result"]
