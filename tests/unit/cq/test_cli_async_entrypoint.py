"""Tests for CQ async CLI entrypoint."""

from __future__ import annotations

import asyncio

import pytest
from tools.cq.cli_app import main_async
from tools.cq.cli_app.app import app


def test_main_async_invokes_meta_run_async(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def fake_run_async(_self: object, **kwargs: object) -> int:
        await asyncio.sleep(0)
        captured.update(kwargs)
        return 9

    monkeypatch.setattr(type(app.meta), "run_async", fake_run_async)
    exit_code = asyncio.run(main_async(["cache"]))
    assert exit_code == 9
    assert captured["tokens"] == ["cache"]
    assert captured["exit_on_error"] is False
    assert captured["print_error"] is True
    assert captured["result_action"] == "return_int_as_exit_code_else_zero"


def test_main_async_normalizes_bool_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run_async(_self: object, **_kwargs: object) -> bool:
        await asyncio.sleep(0)
        return True

    monkeypatch.setattr(type(app.meta), "run_async", fake_run_async)
    assert asyncio.run(main_async(["cache"])) == 0
