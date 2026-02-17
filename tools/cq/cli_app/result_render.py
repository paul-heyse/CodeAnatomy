"""Rendering helpers for CQ CLI result output."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.core.renderers import (
    render_dot,
    render_mermaid_class_diagram,
    render_mermaid_flowchart,
)
from tools.cq.core.report import render_markdown, render_summary_condensed
from tools.cq.core.serialization import dumps_json

if TYPE_CHECKING:
    from tools.cq.cli_app.protocols import ConsolePort
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.schema import CqResult


_JSON_LIKE_FORMATS = {"json", "ldmd", "dot", "mermaid", "mermaid-class"}


def render_result(
    result: CqResult,
    output_format: OutputFormat,
) -> str:
    """Render a CQ result to the selected output format.

    Returns:
        str: Rendered text payload.
    """
    format_value = str(output_format)

    if format_value == "both":
        md = render_markdown(result)
        js = dumps_json(result, indent=2)
        return f"{md}\n\n---\n\n{js}"

    renderers: dict[str, Callable[[CqResult], str]] = {
        "json": lambda payload: dumps_json(payload, indent=2),
        "md": render_markdown,
        "summary": render_summary_condensed,
        "mermaid": render_mermaid_flowchart,
        "mermaid-class": render_mermaid_class_diagram,
        "dot": render_dot,
    }

    if format_value == "ldmd":
        from tools.cq.ldmd.writer import render_ldmd_from_cq_result

        return render_ldmd_from_cq_result(result)

    renderer = renderers.get(format_value, render_markdown)
    return renderer(result)


def emit_output(
    output: str,
    *,
    output_format: OutputFormat,
    console: ConsolePort,
) -> None:
    """Emit rendered output without introducing wrapping artifacts."""
    format_value = str(output_format)
    if format_value in _JSON_LIKE_FORMATS:
        stream = console.file
        stream.write(output)
        if not output.endswith("\n"):
            stream.write("\n")
        stream.flush()
        return
    console.print(output)


__all__ = ["emit_output", "render_result"]
