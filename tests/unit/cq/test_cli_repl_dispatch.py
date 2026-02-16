"""Tests for CQ REPL dispatcher and in-shell help wiring."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from tools.cq.cli_app.app import app
from tools.cq.cli_app.commands.repl import _dispatch_with_ctx, repl, repl_help
from tools.cq.cli_app.context import CliContext

REPL_DISPATCH_EXIT_CODE = 7


def test_dispatch_with_ctx_injects_context(tmp_path: Path) -> None:
    """Test dispatch with ctx injects context."""
    ctx = CliContext.build(argv=["cq", "repl"], root=tmp_path)

    def command(*, ctx: object | None = None) -> int:
        assert ctx is not None
        return REPL_DISPATCH_EXIT_CODE

    bound = inspect.signature(command).bind()
    exit_code = _dispatch_with_ctx(ctx, command, bound, {"ctx": None})
    assert exit_code == REPL_DISPATCH_EXIT_CODE
    assert bound.arguments["ctx"] is ctx


def test_repl_registers_dispatcher(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test repl registers dispatcher."""
    captured: dict[str, object] = {}

    def fake_interactive_shell(_self: object, **kwargs: object) -> None:
        captured.update(kwargs)

    ctx = CliContext.build(argv=["cq", "repl"], root=tmp_path)
    monkeypatch.setattr(type(app), "interactive_shell", fake_interactive_shell)
    assert repl(ctx=ctx) == 0
    assert captured["prompt"] == "cq> "
    assert captured["exit_on_error"] is False
    assert callable(captured["dispatcher"])


def test_repl_help_prints_with_tokens(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test repl help prints with tokens."""
    captured: dict[str, object] = {}

    def fake_help_print(_self: object, *, tokens: list[str], **_kwargs: object) -> None:
        captured["tokens"] = tokens

    ctx = CliContext.build(argv=["cq", "help"], root=tmp_path)
    monkeypatch.setattr(type(app), "help_print", fake_help_print)
    assert repl_help("run", ctx=ctx) == 0
    assert captured["tokens"] == ["run"]
