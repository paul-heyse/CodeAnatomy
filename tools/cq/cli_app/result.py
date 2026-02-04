"""Result rendering and action pipeline for cq CLI."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.core.artifacts import save_artifact_json
from tools.cq.core.findings_table import (
    apply_filters,
    build_frame,
    flatten_result,
    rehydrate_result,
)
from tools.cq.core.renderers import (
    render_dot,
    render_mermaid_class_diagram,
    render_mermaid_flowchart,
)
from tools.cq.core.report import render_markdown, render_summary
from tools.cq.core.serialization import dumps_json

if TYPE_CHECKING:
    from tools.cq.cli_app.context import CliResult, FilterConfig
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.schema import CqResult


def apply_result_filters(result: CqResult, filters: FilterConfig) -> CqResult:
    """Apply CLI filter options to a result.

    Parameters
    ----------
    result
        Original analysis result.
    filters
        Filter configuration.

    Returns
    -------
    CqResult
        Filtered result.
    """
    if not filters.has_filters:
        return result

    records = flatten_result(result)
    if not records:
        return result

    df = build_frame(records)
    filtered_df = apply_filters(
        df,
        include=filters.include if filters.include else None,
        exclude=filters.exclude if filters.exclude else None,
        impact=[str(b) for b in filters.impact] if filters.impact else None,
        confidence=[str(b) for b in filters.confidence] if filters.confidence else None,
        severity=[str(s) for s in filters.severity] if filters.severity else None,
        limit=filters.limit,
    )
    return rehydrate_result(result, filtered_df)


def render_result(
    result: CqResult,
    output_format: OutputFormat,
) -> str:
    """Render a result to string in the specified format.

    Parameters
    ----------
    result
        Analysis result.
    output_format
        Output format.

    Returns
    -------
    str
        Rendered output.
    """
    format_value = str(output_format)

    if format_value == "both":
        md = render_markdown(result)
        js = dumps_json(result, indent=2)
        return f"{md}\n\n---\n\n{js}"

    renderers: dict[str, Callable[[CqResult], str]] = {
        "json": lambda payload: dumps_json(payload, indent=2),
        "md": render_markdown,
        "summary": render_summary,
        "mermaid": render_mermaid_flowchart,
        "mermaid-class": render_mermaid_class_diagram,
        "dot": render_dot,
    }
    renderer = renderers.get(format_value, render_markdown)
    return renderer(result)


def handle_result(cli_result: CliResult, filters: FilterConfig | None = None) -> int:
    """Handle a CLI result using context-based configuration.

    This function uses the output settings from the CliContext to render
    and save results, providing a unified output handling mechanism.

    Parameters
    ----------
    cli_result
        The CLI result containing the result data and context.
    filters
        Optional filter configuration. If not provided, uses filters
        from the CliResult or an empty FilterConfig.

    Returns
    -------
    int
        Exit code (0 for success).
    """
    from tools.cq.cli_app.context import FilterConfig
    from tools.cq.cli_app.types import OutputFormat

    # For non-CqResult (e.g., admin command with int exit code)
    if not cli_result.is_cq_result:
        return cli_result.get_exit_code()

    ctx = cli_result.context
    result = cli_result.result

    # Use context settings or defaults
    output_format = ctx.output_format if ctx.output_format else OutputFormat.md
    artifact_dir = str(ctx.artifact_dir) if ctx.artifact_dir else None
    no_save = not ctx.save_artifact

    # Determine filters: explicit param > result.filters > empty
    if filters is None:
        filters = cli_result.filters if cli_result.filters else FilterConfig()

    # Apply filters
    result = apply_result_filters(result, filters)

    # Save artifact unless disabled
    if not no_save:
        artifact = save_artifact_json(result, artifact_dir)
        result.artifacts.append(artifact)

    # Render and output
    output = render_result(result, output_format)
    sys.stdout.write(f"{output}\n")

    return 0
