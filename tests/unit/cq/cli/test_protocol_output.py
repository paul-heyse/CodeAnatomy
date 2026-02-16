"""Tests for CLI protocol output helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.context import CliContext, CliTextResult
from tools.cq.cli_app.protocol_output import emit_payload, json_result, text_result, wants_json
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.toolchain import Toolchain

ERROR_EXIT_CODE = 2


def test_wants_json_true() -> None:
    """Test wants_json returns True when output format is JSON."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
        output_format=OutputFormat.json,
    )
    assert wants_json(ctx) is True


def test_wants_json_false() -> None:
    """Test wants_json returns False when output format is not JSON."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
        output_format=OutputFormat.md,
    )
    assert wants_json(ctx) is False


def test_wants_json_none() -> None:
    """Test wants_json returns False when output format is None."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
        output_format=None,
    )
    assert wants_json(ctx) is False


def test_text_result_default() -> None:
    """Test text_result with default parameters."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
    )
    result = text_result(ctx, "Hello, world!")

    assert result.context is ctx
    assert result.exit_code == 0
    assert result.filters is None
    assert isinstance(result.result, CliTextResult)
    assert result.result.text == "Hello, world!"
    assert result.result.media_type == "text/plain"


def test_text_result_custom_media_type() -> None:
    """Test text_result with custom media type."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
    )
    result = text_result(ctx, "<html></html>", media_type="text/html")

    assert isinstance(result.result, CliTextResult)
    assert result.result.text == "<html></html>"
    assert result.result.media_type == "text/html"


def test_text_result_custom_exit_code() -> None:
    """Test text_result with custom exit code."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
    )
    result = text_result(ctx, "Error occurred", exit_code=ERROR_EXIT_CODE)

    assert result.exit_code == ERROR_EXIT_CODE
    assert result.get_exit_code() == ERROR_EXIT_CODE


def test_json_result_default() -> None:
    """Test json_result with default parameters."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
    )
    payload = {"status": "ok", "count": 42}
    result = json_result(ctx, payload)

    assert result.context is ctx
    assert result.exit_code == 0
    assert result.filters is None
    assert isinstance(result.result, CliTextResult)
    assert result.result.media_type == "application/json"
    assert '"status": "ok"' in result.result.text
    assert '"count": 42' in result.result.text


def test_json_result_custom_exit_code() -> None:
    """Test json_result with custom exit code."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
    )
    payload = {"error": "not found"}
    result = json_result(ctx, payload, exit_code=ERROR_EXIT_CODE)

    assert result.exit_code == ERROR_EXIT_CODE
    assert result.get_exit_code() == ERROR_EXIT_CODE
    assert isinstance(result.result, CliTextResult)
    assert '"error": "not found"' in result.result.text


def test_json_result_complex_payload() -> None:
    """Test json_result with complex nested payload."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
    )
    payload = {
        "sections": [
            {"id": "s1", "depth": 1},
            {"id": "s2", "depth": 2},
        ],
        "total_bytes": 1024,
    }
    result = json_result(ctx, payload)

    assert isinstance(result.result, CliTextResult)
    assert result.result.media_type == "application/json"
    assert '"sections"' in result.result.text
    assert '"total_bytes": 1024' in result.result.text


def test_emit_payload_json_output_uses_json_result() -> None:
    """emit_payload should return JSON media type when output format requests JSON."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
        output_format=OutputFormat.json,
    )
    result = emit_payload(ctx, {"message": "ok"})
    assert isinstance(result.result, CliTextResult)
    assert result.result.media_type == "application/json"
    assert '"message": "ok"' in result.result.text


def test_emit_payload_prefers_message_for_text_fallback() -> None:
    """emit_payload should use payload.message for text fallback in non-JSON output."""
    ctx = CliContext(
        argv=["cq", "test"],
        root=Path("/tmp"),
        toolchain=Toolchain.detect(),
        output_format=OutputFormat.md,
    )
    result = emit_payload(ctx, {"deprecated": True, "message": "legacy"})
    assert isinstance(result.result, CliTextResult)
    assert result.result.media_type == "text/plain"
    assert result.result.text == "legacy"
