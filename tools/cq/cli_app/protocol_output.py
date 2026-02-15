"""Shared protocol output helpers for CLI commands."""

from __future__ import annotations

import json

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.types import OutputFormat


def wants_json(ctx: CliContext) -> bool:
    """Check if the CLI context requests JSON output.

    Args:
        ctx: CLI context with output format preference.

    Returns:
        bool: True if JSON output is requested.
    """
    return ctx.output_format == OutputFormat.json


def text_result(
    ctx: CliContext,
    text: str,
    *,
    media_type: str = "text/plain",
    exit_code: int = 0,
) -> CliResult:
    """Build a CLI text result handled by unified output pipeline.

    Args:
        ctx: CLI context.
        text: Text content to output.
        media_type: MIME type of the content.
        exit_code: Exit code for the result.

    Returns:
        CliResult: Wrapped CLI result with text payload.
    """
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


def json_result(
    ctx: CliContext,
    payload: object,
    *,
    exit_code: int = 0,
) -> CliResult:
    """Build a CLI result with JSON-serialized payload.

    Args:
        ctx: CLI context.
        payload: Object to serialize as JSON.
        exit_code: Exit code for the result.

    Returns:
        CliResult: Wrapped CLI result with JSON text payload.
    """
    serialized = json.dumps(payload, indent=2)
    return CliResult(
        result=CliTextResult(text=serialized, media_type="application/json"),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


__all__ = [
    "json_result",
    "text_result",
    "wants_json",
]
