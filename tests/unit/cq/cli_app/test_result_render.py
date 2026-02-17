"""Tests for CLI result rendering helpers."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import cast

import pytest
from tools.cq.cli_app.protocols import ConsolePort
from tools.cq.cli_app.result_render import emit_output, render_result
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.schema import CqResult, RunMeta


@dataclass
class _Console:
    file: io.StringIO
    printed: list[str]

    def print(self, *args: object, **kwargs: object) -> None:
        _ = kwargs
        self.printed.append(" ".join(str(arg) for arg in args))


def _result() -> CqResult:
    return CqResult(
        run=RunMeta(
            macro="search",
            argv=["cq", "search", "target"],
            root=".",
            started_ms=0.0,
            elapsed_ms=1.0,
            toolchain={},
        )
    )


def test_render_result_both_combines_markdown_and_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render both output format with markdown and JSON sections."""
    result = _result()
    monkeypatch.setattr("tools.cq.cli_app.result_render.render_markdown", lambda _r: "MD")
    monkeypatch.setattr(
        "tools.cq.cli_app.result_render.dumps_json",
        lambda _r, **_kwargs: "JSON",
    )

    rendered = render_result(result, OutputFormat.both)

    assert rendered == "MD\n\n---\n\nJSON"


def test_emit_output_writes_json_like_directly() -> None:
    """Write JSON-like output directly to the sink file stream."""
    sink = io.StringIO()
    console = _Console(file=sink, printed=[])

    emit_output(
        '{"ok":true}',
        output_format=OutputFormat.json,
        console=cast("ConsolePort", console),
    )

    assert sink.getvalue() == '{"ok":true}\n'
    assert console.printed == []


def test_emit_output_uses_console_print_for_markdown() -> None:
    """Use console printing path for markdown output."""
    sink = io.StringIO()
    console = _Console(file=sink, printed=[])

    emit_output("hello", output_format=OutputFormat.md, console=cast("ConsolePort", console))

    assert console.printed == ["hello"]
