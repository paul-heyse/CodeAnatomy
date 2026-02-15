"""Shared protocol output helpers for CLI commands."""

from __future__ import annotations

import msgspec

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.types import OutputFormat

_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")


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
    serialized = _serialize_json(payload)
    return CliResult(
        result=CliTextResult(text=serialized, media_type="application/json"),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


def emit_payload(
    ctx: CliContext,
    payload: object,
    *,
    text_fallback: str | None = None,
    exit_code: int = 0,
) -> CliResult:
    """Emit payload as JSON or text based on output format.

    Returns:
        CliResult: Serialized response in the requested output format.
    """
    if wants_json(ctx):
        return json_result(ctx, payload, exit_code=exit_code)

    if text_fallback is None and isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message:
            text_fallback = message
    if text_fallback is None:
        text_fallback = _serialize_json(payload)

    return text_result(ctx, text_fallback, exit_code=exit_code)


def _serialize_json(payload: object) -> str:
    encoded = _JSON_ENCODER.encode(payload)
    return msgspec.json.format(encoded, indent=2).decode("utf-8")


__all__ = [
    "emit_payload",
    "json_result",
    "text_result",
    "wants_json",
]
